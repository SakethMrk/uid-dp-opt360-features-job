# Apache Flink: Stateful Stream Processing Guide

This guide concisely covers the core mechanics of Flink's stateful operations, designed to get you building robust, fault-tolerant streams quickly.

---

## 1. Stateful Stream Processing
In traditional stateless processing (like Spring Boot reading Kafka), events are isolated. In **stateful** stream processing, Flink acts as the database.
When you call `stream.keyBy(event -> event.getUserId())`, Flink routes every event for that specific user to the exact same physical node. This means you can keep local memory counters and objects specific to that user without needing external databases, removing network latency entirely.

## 2. State Management & RocksDB
You cannot keep billions of users in a Java `HashMap`; the JVM Garbage Collector will crash your nodes.
Flink solves this using **RocksDB**, an embedded database running off-heap on every TaskManager.

### ValueState and Descriptors
```java
// Inside open()
ValueStateDescriptor<Double> desc = new ValueStateDescriptor<>("sum", Double.class);
desc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
myState = getRuntimeContext().getState(desc);
```
- **Read**: `myState.value()` deserializes from RocksDB into Java.
- **Write**: `myState.update(newVal)` serializes Java to RocksDB.
Always fetch the state once, manipulate the variable, and update once at the end.

### RocksDB Compaction
RocksDB writes to fast Memory Tables, then flushes immutable files (SST files) to disk. Periodically, it merges overlapping files in a process called **Compaction**.
*Warning*: Heavy writes cause heavy Compaction, which eats CPU. If your cluster suddenly lags and CPU spikes to 100%, RocksDB compaction is happening.

## 3. Checkpointing & State Recovery
To ensure you never lose your RocksDB state if a node crashes, Flink uses Distributed Snapshots (Checkpointing).

1. **The Barrier**: The JobManager injects a "Checkpoint Barrier" into Kafka.
2. **The Flush**: As the barrier flows through your operators, each operator pauses processing, flushes its current RocksDB state to AWS S3 (or HDFS), and passes the barrier forward.
3. **Exactly-Once Recovery**: If a node explodes, Flink kills the job, downloads the last successful state from S3 to the new node, rewinds Kafka to that exact second, and resumes. Zero double-counting.

**Golden Rule (`uid`)**:
Always append `.uid("my-feature-name")` to your process operators! If you don't, Flink generates a random hash based on the code structure. If you change your code later, the hash changes, and Flink deletes all your saved S3 state because it cannot map the old hash to the new code.

## 4. KeyedProcessFunction Mastery
This is the most powerful API in Flink. It gives you 100% control over state and time.

```java
public class MyFeature extends KeyedProcessFunction<String, Event, Alert> {
    private transient ValueState<Integer> countState;

    @Override
    public void open(Configuration parameters) {
        // Initialize state here
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<Alert> out) throws Exception {
        // 1. Read State
        Integer count = countState.value();
        if (count == null) count = 0;

        // 2. Logic
        count++;
        
        // 3. Register a Timer to clean up or alert in the future
        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + (60 * 1000L)); // 1 min

        // 4. Update State
        countState.update(count);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
        // Fires precisely when the Time has passed
        countState.clear();
    }
}
```

### Understanding `onTimer()` and Watermarks
You cannot use `Thread.sleep()` in stream processing. Instead, you use the `TimerService`.

When you register an Event Time timer natively `ctx.timerService().registerEventTimeTimer(targetTime)`, Flink waits until the global stream clock (the **Watermark**) passes that timestamp, and then fires `onTimer()`.
- **Deduplication**: If you register the exact same timestamp 100 times for the same user, the timer structurally deducts duplicates and fires strictly exactly **once**.
- **Memory Safety**: Use `onTimer()` to call `state.clear()` so you don't leak memory for users who leave your platform permanently.
