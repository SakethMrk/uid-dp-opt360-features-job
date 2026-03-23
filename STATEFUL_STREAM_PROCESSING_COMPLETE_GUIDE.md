# Apache Flink Stateful Stream Processing — Complete Mastery Guide

> **Purpose**: After reading this document, you will have complete mastery over every aspect of Flink's stateful stream processing — from the physical storage engine to the mathematical guarantees.

---

## Table of Contents

1. [The Big Picture: What Is Stateful Stream Processing?](#1-the-big-picture)
2. [Keys and KeyBy: The Foundation of Everything](#2-keys-and-keyby)
3. [KeyedProcessFunction: The Most Powerful API](#3-keyedprocessfunction)
4. [State Types: ValueState, ListState, MapState](#4-state-types)
5. [State Backends: Memory vs RocksDB](#5-state-backends)
6. [State TTL: Automatic State Expiry](#6-state-ttl)
7. [Event Time, Processing Time, and Ingestion Time](#7-time-semantics)
8. [Watermarks: The Heart of Event-Time Processing](#8-watermarks)
9. [Timers and OnTimer: Scheduled Processing](#9-timers-and-ontimer)
10. [Windows: Tumbling, Sliding, Session](#10-windows)
11. [Checkpointing: Fault Tolerance Guarantees](#11-checkpointing)
12. [Exactly-Once Semantics: End-to-End](#12-exactly-once)
13. [Backpressure: When Downstream Can't Keep Up](#13-backpressure)
14. [Operator State vs Keyed State](#14-operator-vs-keyed-state)
15. [RocksDB Deep Dive: How State Is Physically Stored](#15-rocksdb-deep-dive)
16. [Production Best Practices](#16-production-best-practices)
17. [Common Pitfalls and Anti-Patterns](#17-common-pitfalls)
18. [Our Project's Architecture Explained](#18-our-project-architecture)

---

## 1. The Big Picture

### What Is State, Really?

In a traditional stateless computation (like a `map()` or `filter()`), each event is processed independently. The function has no memory of what came before.

**Stateful** processing means the function REMEMBERS previous events. It maintains a "state" — a persistent, fault-tolerant variable — that survives across events and even across failures.

```
STATELESS: event → function → output
           (each event processed independently)

STATEFUL:  event → function(state) → output
                      ↑        ↓
                      └────────┘
                   state persists across events
```

### Why Is This Hard?

Three problems make stateful streaming non-trivial:

1. **Scale**: State must be partitioned across parallel instances. If you're processing 10,000 opt_ids, they must be distributed across workers WITHOUT mixing up state.

2. **Fault Tolerance**: If a worker crashes, its state CANNOT be lost. And when it recovers, it must process events exactly where it left off — no duplicates, no gaps.

3. **Time**: Events arrive out of order. An event with timestamp 10:00 might arrive AFTER an event with timestamp 10:05. The system must handle this correctly.

Flink solves ALL THREE problems. This document explains how.

---

## 2. Keys and KeyBy

### The Core Concept

`keyBy()` is the most important operation in Flink. It partitions the stream by a key, ensuring that ALL events with the same key go to the SAME operator instance. This is REQUIRED for keyed state.

```java
stream.keyBy(InputMessageTxn::getOptId)  // All events for the same opt_id go to the same instance
      .process(new MyKeyedProcessFunction())
```

### What Happens Internally

When you call `keyBy()`:

1. Flink computes a **hash** of the key value
2. The hash determines which **key group** the event belongs to
3. Key groups are assigned to operator **subtasks** (parallel instances)
4. Each subtask has its OWN state store, isolated from other subtasks

```
Input Stream:
  [opt_id=A, ...], [opt_id=B, ...], [opt_id=A, ...], [opt_id=C, ...]

After keyBy(optId):
  Subtask 0: [opt_id=A], [opt_id=A]    ← all A's together
  Subtask 1: [opt_id=B]                ← all B's together
  Subtask 2: [opt_id=C]                ← all C's together
```

### Key Facts About Keys

| Fact | Implication |
|------|-------------|
| Keys are hashed | Two different strings CAN hash to the same subtask (but their STATE is separate) |
| Max parallelism = max key groups | Default: 128. Can be set up to 32768 |
| Key must be serializable | Use simple types (String, Integer, Long, Tuple) |
| Null keys are NOT allowed | Will throw runtime exception |
| Key distribution affects performance | Hot keys (one opt_id with millions of events) cause skew |

### Key Groups vs Subtasks

A **key group** is Flink's finest granularity for state assignment. With max parallelism = 128:
- There are 128 key groups
- If you have 4 parallel subtasks, each subtask handles ~32 key groups
- When rescaling (changing parallelism), key groups are redistributed

```
Parallelism 4:
  Subtask 0: Key groups 0-31
  Subtask 1: Key groups 32-63
  Subtask 2: Key groups 64-95
  Subtask 3: Key groups 96-127

Rescaled to 8:
  Subtask 0: Key groups 0-15
  Subtask 1: Key groups 16-31
  Subtask 2: Key groups 32-47
  ...
```

This is how Flink achieves **rescaling without data loss** — key groups are atomic units.

---

## 3. KeyedProcessFunction: The Most Powerful API

### Why It's the Most Powerful

`KeyedProcessFunction<K, I, O>` gives you access to:
- The current **key** being processed
- The **state** (ValueState, MapState, etc.)
- **Timers** (schedule callbacks in the future)
- The **context** (current timestamp, watermark, etc.)
- **Side outputs** (emit to multiple output streams)

Every other API (map, filter, window) can be implemented WITH KeyedProcessFunction. The reverse is NOT true.

### Anatomy

```java
public class MyFeature extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    // Declare state handles (transient = not serialized with the operator)
    private transient ValueState<MyState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        // Initialize state handles ONCE when the operator starts
        // This is called BEFORE any events are processed
        super.open(parameters);
        
        ValueStateDescriptor<MyState> descriptor = new ValueStateDescriptor<>(
            "myStateDescriptor",
            TypeInformation.of(new TypeHint<MyState>() {})
        );
        
        // Optional: Enable TTL
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(48))
            .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
            .build());
        
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) 
            throws Exception {
        // Called for EVERY event with the SAME key
        // ctx.getCurrentKey()  → the key value
        // ctx.timestamp()      → the event-time timestamp (from watermark strategy)
        // state.value()        → read the current state (null if never set)
        // state.update(...)    → write new state
        // state.clear()        → delete state
        // out.collect(...)     → emit an output event
        // ctx.timerService().registerEventTimeTimer(ts)  → schedule a timer
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OutMessage> out) 
            throws Exception {
        // Called when a registered timer fires
        // Same capabilities as processElement
    }
}
```

### The `transient` Keyword

State handles are declared as `transient` because:
- They are NOT part of the operator's serialized state
- They are REFERENCES to state stored in the state backend
- They are re-initialized in `open()` after recovery from a checkpoint

```java
private transient ValueState<MyState> state;  // ✅ Correct: re-initialized in open()
private ValueState<MyState> state;            // ❌ Wrong: would try to serialize the handle
```

### Context Object

The `Context` (in `processElement`) provides:

| Method | Returns | Description |
|--------|---------|-------------|
| `getCurrentKey()` | `K` | The key this event was partitioned by |
| `timestamp()` | `long` | The event-time timestamp assigned by WatermarkStrategy |
| `timerService()` | `TimerService` | Register/delete event-time or processing-time timers |
| `output(OutputTag, value)` | `void` | Emit to a side output |

---

## 4. State Types

### ValueState<T>

The simplest and most used. Stores ONE value per key.

```java
ValueState<Integer> counter;

// Read
Integer count = counter.value();    // null if never set
// Write
counter.update(count + 1);
// Delete
counter.clear();
```

**When to use**: Single aggregate per key (running count, last timestamp, POJO with multiple fields).

### ListState<T>

Stores an ordered list of values per key.

```java
ListState<Long> timestamps;

timestamps.add(System.currentTimeMillis());           // Append
Iterable<Long> all = timestamps.get();                 // Read all
timestamps.update(Arrays.asList(1L, 2L, 3L));         // Replace all
timestamps.clear();                                     // Delete
```

**When to use**: Ordered event logs, sliding window buffers. **Warning**: Can grow unbounded — always implement size limits.

### MapState<K, V>

Stores a key-value map per key. Most flexible but most complex.

```java
MapState<String, Integer> deviceCounts;

deviceCounts.put("DEV_001", 5);                        // Put
Integer count = deviceCounts.get("DEV_001");           // Get (null if absent)
boolean exists = deviceCounts.contains("DEV_001");     // Check
deviceCounts.remove("DEV_001");                        // Remove
Iterable<Map.Entry<String, Integer>> all = deviceCounts.entries(); // Iterate
deviceCounts.clear();                                  // Delete all
```

**When to use**: Deduplication maps, frequency tables, multi-dimensional state. **Warning**: Iteration is expensive with RocksDB (each entry = separate key-value lookup).

### ReducingState<T> and AggregatingState<T>

Pre-aggregated state — Flink merges values automatically.

```java
ReducingState<Long> sum;  // Configured with a ReduceFunction
sum.add(5L);              // Internally does: currentState = reduceFunction(currentState, 5L)
Long result = sum.get();  // Returns the accumulated result
```

**When to use**: Pre-aggregation where you never need to iterate individual elements.

### State Comparison Table

| State Type | Read Cost | Write Cost | Memory | Use Case |
|-----------|-----------|------------|--------|----------|
| ValueState | O(1) | O(1) | Fixed | Single POJO per key |
| ListState | O(n) | O(1) append | Variable | Event buffers |
| MapState | O(1) per get | O(1) per put | Variable | Lookup tables, dedup |
| ReducingState | O(1) | O(1) | Fixed | Pre-aggregated metrics |

---

## 5. State Backends

### HeapStateBackend (FsStateBackend)

- State lives **in JVM heap memory**
- Very fast access (native Java objects)
- Limited by available JVM memory
- Checkpointing: serializes the entire state to the checkpoint store (filesystem/S3)

**When to use**: Small state, low latency requirements, development/testing.

### RocksDB State Backend

- State lives **on local disk** (embedded RocksDB database)
- Access requires **serialization/deserialization** for every read/write
- Can handle **state much larger than available memory** (TB-scale)
- Checkpointing: **incremental** — only changed SST files are uploaded

**When to use**: Production workloads with large state. This is what our project uses.

```java
EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend(true); // true = incremental
environment.setStateBackend(stateBackend);
```

### How RocksDB Works (Simplified)

```
Write Path:
  1. Write to MemTable (in-memory sorted buffer)
  2. When MemTable is full → flush to SST file on disk (Level 0)
  3. Background compaction merges SST files into sorted runs

Read Path:
  1. Check MemTable (fastest)
  2. Check Block Cache (LRU cache of SST blocks)
  3. Check SST files via Bloom filters (disk I/O)
```

### Performance Characteristics

| Operation | Heap Backend | RocksDB Backend |
|-----------|-------------|-----------------|
| Read ValueState | ~10ns | ~1-10μs |
| Write ValueState | ~10ns | ~1-10μs |
| Iterate MapState | ~10ns/entry | ~10-100μs/entry |
| Checkpoint (full) | Slow (serialize all) | N/A |
| Checkpoint (incremental) | N/A | Fast (only changed SSTs) |
| Max state size | ~16GB (JVM heap) | ~TBs (disk) |

---

## 6. State TTL

### What It Does

State TTL (Time-To-Live) automatically **expires** state entries that haven't been accessed within a configured duration. This prevents unbounded state growth.

```java
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.hours(48))                                    // Expire after 48 hours
    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)       // Reset timer on any access
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // Never return stale data
    .cleanupFullSnapshot()                                         // Clean during snapshots
    .build();

descriptor.enableTimeToLive(ttlConfig);
```

### Update Types

| Type | Behavior | When To Use |
|------|----------|-------------|
| `OnCreateAndWrite` | Timer resets only when state is created or written | Data should expire even if read frequently |
| `OnReadAndWrite` | Timer resets on ANY access (read or write) | "Active" state should stay alive (our choice) |

### State Visibility

| Visibility | Behavior |
|-----------|----------|
| `NeverReturnExpired` | Returns `null` if state has expired, even if not yet cleaned |
| `ReturnExpiredIfNotCleanedUp` | Returns expired state until background cleanup removes it |

### Cleanup Strategies

| Strategy | Mechanism | Trade-off |
|----------|-----------|-----------|
| `cleanupFullSnapshot()` | Cleaned during full checkpoints | No runtime overhead, but expired state lingers between checkpoints |
| `cleanupIncrementally(N, flag)` | Checks N entries per access | Consistent cleanup, slight per-access overhead |
| `cleanupInRocksdbCompactFilter()` | Cleaned during RocksDB compaction | Leverages RocksDB's natural maintenance, RocksDB-only |

### Our Project's TTL Configuration

```java
// 48-hour TTL (default for most features)
public static StateTtlConfig stateTtlFunction() {
    return StateTtlConfig.newBuilder(Time.hours(48))
        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .cleanupFullSnapshot()
        .build();
}

// 24-hour TTL (for high-frequency features)
public static StateTtlConfig stateTtlFunction1Day() {
    return StateTtlConfig.newBuilder(Time.hours(24))
        ...
}
```

---

## 7. Time Semantics

### The Three Clocks

| Clock | Source | Characteristic |
|-------|--------|----------------|
| **Event Time** | Embedded in the event itself | Deterministic, reproducible, handles out-of-order |
| **Processing Time** | Wall clock of the machine | Simple, no latency, non-deterministic |
| **Ingestion Time** | Assigned when event enters Flink | Compromise between the two |

### Why Event Time Matters

Consider two events:
```
Event A: timestamp=10:00:00, arrival=10:00:05
Event B: timestamp=10:00:01, arrival=10:00:03
```

With **Processing Time**: B is processed before A (arrived first). If we're counting events per minute, we get the wrong count.

With **Event Time**: A is processed before B (event time 10:00:00 < 10:00:01). Correct ordering guaranteed.

### How Event Time Is Assigned

You tell Flink which field in your event contains the timestamp:

```java
WatermarkStrategy
    .<InputMessageTxn>forBoundedOutOfOrderness(Duration.ofSeconds(60))
    .withTimestampAssigner((event, recordTimestamp) -> {
        // Extract the event time from the Kafka record timestamp
        return event.getKafkaSourceTimestamp();
    });
```

After this, `ctx.timestamp()` in `processElement()` returns this extracted timestamp.

---

## 8. Watermarks: The Heart of Event-Time Processing

### What Is a Watermark?

A watermark is a **declaration** by the system:

> "I guarantee that no more events with timestamp ≤ W will arrive."

When the watermark reaches time W, Flink knows it's safe to evaluate all logic that depends on events up to time W.

```
Event Stream:  [t=10] [t=12] [t=8] [t=15] [t=11]  ...
Watermarks:         W=10       W=8*   W=15   W=11*

*Watermark never goes backward — it stays at max seen timestamp minus tolerance
```

### Watermark Strategies

#### 1. Monotonously Increasing (No Out-of-Order)

```java
WatermarkStrategy.forMonotonousTimestamps()
```

- Watermark = latest event timestamp
- ANY out-of-order event is considered "late" (dropped or handled by late data policy)
- **Use when**: Events are guaranteed in order (rare in production)

#### 2. Bounded Out-of-Orderness

```java
WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(60))
```

- Watermark = max(seen timestamps) - 60 seconds
- Events within 60 seconds of the max are processed normally
- Events older than 60 seconds are considered "late"
- **Use when**: Events can arrive up to N seconds late (our recommended choice)

### Watermark Flow Through the Pipeline

```
Source → [W=100] → keyBy → [W=100] → ProcessFunction → [W=100] → Sink

When multiple sources:
  Source1: [W=100]  ─┐
                     ├→ Union: W = min(100, 90) = 90
  Source2: [W=90]   ─┘
```

**Key insight**: In a union/join, the watermark is the MINIMUM across all inputs. A slow source holds back ALL downstream operators.

### Watermark and Timers

When a watermark W arrives at a `KeyedProcessFunction`:
1. Any registered event-time timer with timestamp ≤ W fires
2. The `onTimer()` method is called
3. Inside `onTimer()`, you can safely process buffered events with timestamps ≤ W

This is the foundation of our fault-tolerant features (`FeatureAuthBruteForceCompromise`, `FeatureAuthMultiDeviceConcurrency`).

### Watermark Latency vs Correctness

| Configuration | Latency | Correctness |
|--------------|---------|-------------|
| `forBoundedOutOfOrderness(0s)` | Immediate | Poor (any delay = late data) |
| `forBoundedOutOfOrderness(60s)` | 60 seconds | Good (handles most delays) |
| `forBoundedOutOfOrderness(10min)` | 10 minutes | Excellent (handles extreme delays) |
| `forMonotonousTimestamps()` | Immediate | Only if source guarantees order |

---

## 9. Timers and OnTimer

### The Two Timer Types

| Timer | Fires When | Use Case |
|-------|-----------|----------|
| **Event-Time Timer** | Watermark passes the registered timestamp | Process buffered events, window evaluation |
| **Processing-Time Timer** | Wall clock reaches the registered timestamp | Timeouts, periodic side-effects |

### Registering Timers

```java
// In processElement():
ctx.timerService().registerEventTimeTimer(eventTimestamp);     // Event-time
ctx.timerService().registerProcessingTimeTimer(currentTime + 60000); // Processing-time

// Delete previously registered timers:
ctx.timerService().deleteEventTimeTimer(eventTimestamp);
ctx.timerService().deleteProcessingTimeTimer(time);
```

### Timer Coalescing

If you register multiple timers for the SAME timestamp, `onTimer` is called ONCE. Flink deduplicates timers.

```java
ctx.timerService().registerEventTimeTimer(1000L);
ctx.timerService().registerEventTimeTimer(1000L);
ctx.timerService().registerEventTimeTimer(1000L);
// onTimer(1000L, ...) is called ONCE
```

### Timer Lifecycle

```
1. processElement() registers timer at timestamp T
2. Flink stores the timer in a priority queue (per key)
3. When watermark >= T, onTimer(T) fires for that key
4. onTimer() can:
   - Read/write state (same key context)
   - Emit events via collector
   - Register NEW timers
   - Clear state
```

### The Buffer-and-Sort Pattern (Fault-Tolerant Out-of-Order Handling)

This is the pattern used by our most advanced features:

```java
// In processElement():
//  1. Buffer the event at its event time
eventBuffer.put(eventTime, event);
//  2. Register a timer at the event time
ctx.timerService().registerEventTimeTimer(eventTime);

// In onTimer():
//  The watermark has passed 'timestamp' — ALL events with timestamp <= 'timestamp' have arrived
//  1. Extract buffered events with timestamp <= 'timestamp'
//  2. Sort them chronologically
//  3. Process them in STRICT order
//  4. Remove processed events from buffer
```

**Guarantee**: Events are processed in PERFECT chronological order, regardless of arrival order.

---

## 10. Windows

### Window Types

#### Tumbling Windows

Non-overlapping, fixed-size windows.

```
Window 1: [00:00 - 01:00)
Window 2: [01:00 - 02:00)
Window 3: [02:00 - 03:00)
```

```java
stream.keyBy(...)
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .aggregate(new CountAggregator(), new WindowResultFunction());
```

#### Sliding Windows

Overlapping windows. Each event may belong to multiple windows.

```
Window 1: [00:00 - 00:05)
Window 2: [00:01 - 00:06)
Window 3: [00:02 - 00:07)
(slide = 1 min, size = 5 min → each event in 5 windows)
```

```java
stream.keyBy(...)
      .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
      .aggregate(...)
```

#### Session Windows

Dynamic windows that close after a gap of inactivity.

```
Events: [t=0] [t=1] [t=2]    [t=10] [t=11]    [t=30]
                  └─────────┘           └─────────┘        └──────┘
                   Session 1            Session 2          Session 3
                  (gap > 5 = new session)
```

### Windows vs KeyedProcessFunction

| Feature | Windows | KeyedProcessFunction |
|---------|---------|---------------------|
| Fixed-size aggregation | ✅ Easy | Manual (track window boundaries) |
| Custom logic | Limited | ✅ Full flexibility |
| Multiple state variables | ❌ Single accumulator | ✅ Any number of state types |
| Conditional emission | Limited (emit at window close) | ✅ Emit at any time |
| Timer control | Automatic (window close) | ✅ Full manual control |

**Our project uses both**: `FeatureAuthCounts` uses windows (tumbling 60-min). All other features use `KeyedProcessFunction` for full flexibility.

---

## 11. Checkpointing

### What Is a Checkpoint?

A checkpoint is a **consistent snapshot** of the entire application's state at a specific point in time. It captures:
- All keyed state across all operators
- All operator state
- The source offsets (e.g., Kafka consumer positions)
- Pending output (if using exactly-once sinks)

### How It Works

Flink uses the **Chandy-Lamport algorithm** (barrier-based):

```
1. JobManager triggers checkpoint N
2. Source operators inject BARRIER_N into the stream
3. Each operator:
   a. Receives BARRIER_N
   b. Takes a snapshot of its state
   c. Sends BARRIER_N downstream
4. When ALL sink operators have processed BARRIER_N:
   checkpoint N is COMPLETE
```

### Barrier Alignment

When an operator has multiple inputs:
- It must wait for BARRIER_N from ALL inputs before taking the snapshot
- Events from "fast" inputs are buffered while waiting for "slow" inputs
- This ensures the snapshot is consistent

```
Input 1: [event] [event] [BARRIER] [event] [event]
Input 2: [event] [event] [event] [event] [BARRIER]
                                          ↑
                                          Only here does the operator snapshot
```

### Our Configuration

```java
environment.enableCheckpointing(120000, CheckpointingMode.EXACTLY_ONCE);
//                               ↑                        ↑
//                          Every 2 minutes          Exactly-once guarantee

environment.getCheckpointConfig().setCheckpointTimeout(30 * 60 * 1000);
//                                                     ↑
//                                              30-minute timeout

cfg.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
cfg.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "s3a://bucket/checkpoints/");
```

### Recovery From Checkpoint

When a failure occurs:
1. Flink stops all operators
2. Restores all state from the LATEST COMPLETED checkpoint
3. Resets Kafka consumers to the offsets stored in that checkpoint
4. Restarts processing from there

**Result**: No data loss, no duplicates (with exactly-once sinks).

---

## 12. Exactly-Once Semantics

### The Three Guarantee Levels

| Level | Meaning | Example |
|-------|---------|---------|
| **At-most-once** | Events can be lost, never duplicated | No checkpointing |
| **At-least-once** | Events are never lost, may be duplicated | Checkpointing + non-transactional sink |
| **Exactly-once** | Every event processed exactly once | Checkpointing + 2PC sink |

### End-to-End Exactly-Once

To achieve true exactly-once from source to sink:

1. **Source**: Must support replay from a specific offset (Kafka: ✅)
2. **Processing**: Guaranteed by Flink's checkpointing mechanism
3. **Sink**: Must support transactions (Kafka sink with `DeliveryGuarantee.EXACTLY_ONCE`)

### How Kafka Exactly-Once Works

```
1. On checkpoint N start:
   - Sink begins a Kafka transaction
2. During processing:
   - Records are written to Kafka within the transaction (not visible to consumers)
3. On checkpoint N complete:
   - Sink COMMITS the transaction
   - Records become visible to consumers
4. On failure:
   - Transaction is ABORTED
   - Records are discarded
   - Recovery from checkpoint (N-1) re-processes and creates a new transaction
```

---

## 13. Backpressure

### What Is Backpressure?

When a downstream operator is slower than an upstream operator, events queue up. This is backpressure. Flink handles it by:

1. **Filling the network buffer** between operators
2. When buffers are full, the upstream operator **slows down**
3. This propagates all the way back to the source

### Identifying Backpressure

In the Flink Web UI:
- **High**: > 50% of time spent waiting for downstream
- Operator color turns RED
- Check which operator is the bottleneck (the one with low input rate but high output queue)

### Common Causes and Solutions

| Cause | Symptom | Fix |
|-------|---------|-----|
| Slow state access | All operators slow | Tune RocksDB (block cache, bloom filters) |
| Hot key | One subtask slow | Re-key or pre-aggregate |
| External system call | Operator blocking | Use async I/O |
| Slow sink (DB/Kafka) | Sink backpressured | Batch writes, increase sink parallelism |
| GC pauses | Intermittent slowdowns | Tune JVM memory, reduce heap state |

---

## 14. Operator State vs Keyed State

### Keyed State (What We Use)

- Scoped to the **current key** (e.g., opt_id)
- Only accessible after `keyBy()`
- Automatically partitioned and redistributed on rescale

### Operator State

- Scoped to the **operator instance** (not per key)
- Accessible without `keyBy()`
- Manually managed redistribution on rescale

```java
// Operator state example: Kafka source stores offsets per partition
public class MySource implements CheckpointedFunction {
    private transient ListState<Long> offsetState;
    
    @Override
    public void initializeState(FunctionInitializationContext ctx) {
        offsetState = ctx.getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("offsets", Long.class));
    }
    
    @Override
    public void snapshotState(FunctionSnapshotContext ctx) {
        offsetState.clear();
        offsetState.add(currentOffset);
    }
}
```

### Redistribution Strategies

| Strategy | Behavior on Rescale |
|----------|-------------------|
| `getListState()` (even-split) | State list elements are evenly distributed across new subtasks |
| `getUnionListState()` | Each new subtask gets the FULL list from ALL old subtasks |

---

## 15. RocksDB Deep Dive

### Architecture

```
┌─────────────────────────────────────────┐
│           Application Layer             │
│    ValueState.value() / .update()       │
├─────────────────────────────────────────┤
│         Flink State Backend API         │
│    Serializes/Deserializes POJO ↔ bytes │
├─────────────────────────────────────────┤
│              RocksDB JNI                │
│    Java ↔ C++ bridge                    │
├─────────────────────────────────────────┤
│           RocksDB Engine (C++)          │
│    ┌────────┐                           │
│    │MemTable│ (in-memory write buffer)  │
│    └────┬───┘                           │
│         ↓ flush                         │
│    ┌────────────┐                       │
│    │ Block Cache │ (LRU read cache)     │
│    └────────────┘                       │
│    ┌────────────────────┐               │
│    │ SST Files (L0→L6) │ (sorted runs) │
│    └────────────────────┘               │
└─────────────────────────────────────────┘
```

### Key Internals

| Component | Size | Purpose |
|-----------|------|---------|
| MemTable | 64-256 MB | In-memory write buffer (very fast) |
| Block Cache | 8 MB - 1 GB | LRU cache for recently read SST blocks |
| Bloom Filters | ~10 bits/key | Skip SST files that don't contain the key |
| Write Buffer | 2-6 write buffers | Concurrent write and flush |

### Tuning for Flink

```java
public class Options implements RocksDBOptionsFactory {
    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return currentOptions
            .setIncreaseParallelism(4)
            .setMaxBackgroundJobs(4);
    }
    
    @Override
    public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return currentOptions
            .setWriteBufferSize(64 * 1024 * 1024)     // 64MB per column family
            .setMaxWriteBufferNumber(3)
            .setMinWriteBufferNumberToMerge(1)
            .setTableFormatConfig(new BlockBasedTableConfig()
                .setBlockCacheSize(256 * 1024 * 1024)  // 256MB block cache
                .setBlockSize(16 * 1024)                // 16KB blocks
                .setFilterPolicy(new BloomFilter(10))   // 10 bits per key
            );
    }
}
```

### The Serialization Tax

Every `ValueState.value()` call with RocksDB:
1. Constructs the state key: `<key-group><key><namespace><state-name>`
2. Calls RocksDB's `get()` via JNI
3. Deserializes the byte[] into a Java POJO

Every `ValueState.update()`:
1. Serializes the Java POJO into byte[]
2. Calls RocksDB's `put()` via JNI

**Implication**: Minimize state reads/writes. Read once into a local variable, modify, write back once.

```java
// ❌ BAD: Multiple serialization round-trips
state.value().count++;           // read + deserialize
state.value().total += amount;   // read + deserialize AGAIN
state.update(state.value());     // read + deserialize AGAIN + serialize + write

// ✅ GOOD: Single read, single write
MyState s = state.value();       // one read + deserialize
s.count++;
s.total += amount;
state.update(s);                 // one serialize + write
```

---

## 16. Production Best Practices

### State Management

1. **Always set TTL** on all state descriptors. Without TTL, state for inactive keys accumulates forever.

2. **Bound collections in state**. `Set<String>` or `List<Long>` in state can grow without limit. Always cap at a maximum size with eviction.

3. **Use ValueState with a POJO** over multiple ValueStates. One read of a POJO with 5 fields is faster than 5 reads of 5 separate ValueStates.

4. **Avoid MapState iteration** with RocksDB. Each entry in MapState is a separate RocksDB key-value pair. Iterating is expensive. If you need to iterate, consider using ValueState with a Map POJO instead (trades update granularity for read performance).

### Checkpointing

5. **Set checkpoint interval wisely**. Too frequent = high overhead. Too infrequent = more reprocessing on failure. 1-5 minutes is typical for production.

6. **Enable incremental checkpointing** with RocksDB. Full checkpoints copy all state. Incremental copies only changed SST files.

7. **Set checkpoint timeout** to accommodate large state sizes. If a checkpoint takes longer than the timeout, it's aborted.

8. **Retain checkpoints on cancellation** for manual recovery:
```java
env.getCheckpointConfig().setExternalizedCheckpointCleanup(
    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
);
```

### Monitoring

9. **Monitor checkpointing duration**. Increasing duration = growing state or I/O bottleneck.

10. **Monitor backpressure**. Sustained backpressure degrades latency and can eventually cause out-of-memory.

11. **Monitor state size** via metrics: `rocksdb.estimate-num-keys`, `rocksdb.estimate-live-data-size`.

### Deployment

12. **Set max parallelism at deploy time**. It CANNOT be changed after deployment without losing state compatibility.

13. **Use UID for all operators**:
```java
stream.process(new MyFunction()).uid("my-operator-uid").name("My Operator");
```
Without explicit UIDs, state mapping breaks when you add/remove operators.

14. **Test state migration** when changing state POJOs. Adding new fields is OK (defaults to null/0). Removing or renaming fields breaks compatibility.

---

## 17. Common Pitfalls and Anti-Patterns

### Pitfall 1: Unbounded State Growth

```java
// ❌ DANGER: dedupSet grows forever
Set<String> processedIds = new HashSet<>();
processedIds.add(eventId);
// After 1 million events → 1 million entries → OOM
```

**Fix**: Use State TTL, or implement manual eviction (LRU, time-based).

### Pitfall 2: Forgetting Null Checks

```java
// ❌ NullPointerException if state was never set or expired
state.value().count++;

// ✅ Safe
MyState s = state.value();
if (s == null) s = new MyState();
s.count++;
```

### Pitfall 3: Using System.currentTimeMillis() for Logic

```java
// ❌ Non-deterministic: different on replay
if (System.currentTimeMillis() - lastTs > threshold) { ... }

// ✅ Deterministic: uses event time
if (ctx.timestamp() - lastTs > threshold) { ... }
```

### Pitfall 4: Heavy Computation in processElement

Every millisecond spent in `processElement()` is a millisecond of throughput lost. Avoid:
- Complex string parsing
- External HTTP calls (use Async I/O instead)
- Logging at INFO level for every event (use DEBUG or log selectively)

### Pitfall 5: Not Using uid() on Operators

```java
// ❌ State mapping breaks if operators are reordered
stream.process(new A()).process(new B());

// ✅ State survives operator reordering
stream.process(new A()).uid("operator-a")
      .process(new B()).uid("operator-b");
```

### Pitfall 6: Ignoring Late Data

With `forBoundedOutOfOrderness(60s)`, events arriving more than 60 seconds late are silently dropped. Always consider:
- What's the maximum expected delay in your system?
- Should you use `allowedLateness()` for window operators?
- Should you use side outputs to capture late data?

---

## 18. Our Project's Architecture Explained

### Data Flow

```
Kafka Topic (DE.OPT.AUTH_EVENTS.V1)
       │
       ├─→ KafkaSource<InputMessageTxn>  (TXN events)
       │        │
       │        ├─→ WatermarkStrategy (bounded out-of-orderness)
       │        │
       │        ├─→ keyBy(optId)
       │        │      │
       │        │      ├─→ FeatureAuthCounts (Window: tumbling 60min)
       │        │      ├─→ FeatureAuthGap (KeyedProcessFunction)
       │        │      ├─→ FeatureAuthRetrySession (KeyedProcessFunction)
       │        │      ├─→ ... (40+ more features)
       │        │      └─→ Advanced Features (7 new features)
       │        │
       │        └─→ union(all feature streams)
       │                │
       │                ├─→ CommentsMapper
       │                ├─→ KafkaSink (DE.OPT.AUTH.FEATURES.TEST)
       │                └─→ IcebergSink (Hive Metastore)
       │
       └─→ KafkaSource<InputMessageBio>  (BIO events)
                │
                └─→ ... (Bio features)
```

### State Backend

- **RocksDB** with incremental checkpointing
- State stored on local SSDs, checkpointed to S3
- Checkpoint interval: 120 seconds
- Checkpoint timeout: 30 minutes

### Parallelism

- Default parallelism: 4
- All operators inherit the same parallelism
- Keyed by `opt_id` → state distributed across 4 subtasks

### Fault Tolerance Pattern Used in Our Features

Our features use two patterns:

**Pattern A: Simple Stateful (most features)**
```
processElement(event):
  state = ValueState.value()
  if state == null: state = new State()
  // ... business logic ...
  ValueState.update(state)
```

**Pattern B: Buffer-and-Sort (BruteForceCompromise, MultiDeviceConcurrency)**
```
processElement(event):
  eventBuffer.put(eventTime, event)       // Buffer
  registerEventTimeTimer(eventTime)        // Schedule

onTimer(timestamp):
  orderedEvents = eventBuffer.entries()
    .filter(ts <= timestamp)
    .sort(byTimestamp)                     // Sort
  for (event : orderedEvents):
    evaluateBusinessLogic(event)           // Process in order
    eventBuffer.remove(event.timestamp)    // Cleanup
```

Pattern B provides the strongest guarantee: events are processed in PERFECT chronological order regardless of arrival order. The trade-off is higher state size (buffered events) and latency (must wait for watermark).

---

## Glossary

| Term | Definition |
|------|-----------|
| **Checkpoint** | Consistent snapshot of all state at a point in time |
| **Savepoint** | User-triggered checkpoint, used for upgrades and rescaling |
| **Watermark** | Monotonically increasing timestamp asserting event completeness |
| **Key Group** | Flink's smallest unit of state partitioning (max 32768) |
| **Barrier** | Marker injected into stream to trigger checkpointing |
| **SST File** | Sorted String Table — RocksDB's on-disk storage format |
| **MemTable** | RocksDB's in-memory write buffer |
| **Side Output** | Secondary output stream from a ProcessFunction |
| **TTL** | Time-To-Live — automatic state expiry |
| **Backpressure** | Flow control when downstream is slower than upstream |
| **Subtask** | Parallel instance of an operator |
| **State Backend** | Storage engine for state (Heap or RocksDB) |
| **Incremental Checkpoint** | Only uploads changed SST files (RocksDB only) |
| **Chandy-Lamport** | Algorithm used by Flink for consistent distributed snapshots |
