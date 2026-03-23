package com.foo.bar.functions.advanced;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import com.foo.bar.pipeline.Driver;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

/**
 * ════════════════════════════════════════════════════════════════════════════════
 * Feature: AUTH SUCCESS RATIO CLIFF (Sudden Success Rate Collapse)
 * ════════════════════════════════════════════════════════════════════════════════
 *
 * WHAT IT DETECTS:
 *   Monitors the ROLLING success rate of an identity using two sliding windows:
 *     - HISTORICAL window (50 events): The established baseline success rate
 *     - RECENT window (10 events): The current success rate
 *   Emits when the RECENT success rate drops below the HISTORICAL rate by a
 *   significant margin (CLIFF_THRESHOLD = 40 percentage points).
 *
 * WHY IT MATTERS (Business Value):
 *   This is fundamentally different from a raw failure count or failure rate.
 *   It catches the TRANSITION from healthy to compromised. Consider these scenarios:
 *
 *   Scenario A: A user who has always had 90% success rate (good biometrics,
 *     clean device). Suddenly their recent 10 attempts show 30% success. That's
 *     a 60-point cliff → indicates biometric data degradation, device tampering,
 *     or credential compromise.
 *
 *   Scenario B: A user who has always had 40% success rate (poor quality sensor).
 *     Their recent 10 attempts show 30% success. Only a 10-point dip → normal
 *     variation, NOT flagged. This is why comparing against the PERSONAL baseline
 *     matters — the existing FeatureAuthFailureRate uses absolute thresholds which
 *     would flag this user all the time.
 *
 *   Real-world attack pattern captured: Attacker initially succeeds with stolen
 *   biometrics (high success rate), then the system starts flagging them (liveness
 *   checks, device checks), causing sudden failures. The cliff captures this
 *   transition point precisely.
 *
 * MATHEMATICAL MODEL:
 *   historical_rate = count_of_successes_in_last_50 / 50
 *   recent_rate     = count_of_successes_in_last_10 / 10
 *   cliff_size      = historical_rate - recent_rate
 *   IF cliff_size >= CLIFF_THRESHOLD → EMIT
 *
 * EDGE CASES HANDLED:
 *   1. Insufficient history: Requires HISTORICAL_WINDOW full (50) before evaluating
 *   2. Kafka duplicates: eventId-based dedup
 *   3. Null authResult: Skipped
 *   4. Non-Y/N authResult: Treated as failure (conservative)
 *   5. One emission per cliff: After emitting, sets cooldown flag that resets only
 *      when the recent rate recovers above (historical - CLIFF_THRESHOLD/2)
 *   6. State size: Fixed-size circular buffer (no unbounded growth)
 *   7. Out-of-order events: Accepted (success rate is order-independent for windows)
 *
 * EMITTED FEATURE:
 *   - Feature Name : auth_success_ratio_cliff
 *   - Feature Value: Cliff size in percentage points (e.g., 60.0 means 60% drop)
 *   - Comments     : JSON with historical_rate, recent_rate, cliff_size, window details
 */
@Slf4j
public class FeatureAuthSuccessRatioCliff extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** Historical baseline window size */
    private static final int HISTORICAL_WINDOW = 50;

    /** Recent evaluation window size */
    private static final int RECENT_WINDOW = 10;

    /** Cliff threshold: minimum % point drop to trigger alert */
    private static final double CLIFF_THRESHOLD = 40.0;

    /** Recovery threshold: recent rate must recover this much to re-enable alerts */
    private static final double RECOVERY_THRESHOLD = 20.0;

    public static class CliffState {
        /**
         * Circular buffer of auth results. true = success, false = failure.
         * Size bounded to HISTORICAL_WINDOW.
         */
        public LinkedList<Boolean> resultHistory = new LinkedList<>();
        public int totalSuccessInHistory = 0;
        public boolean cliffAlerted = false;
        public String lastProcessedEventId = "";
        public long lastEventTimestamp = 0L;
    }

    private transient ValueState<CliffState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<CliffState> descriptor = new ValueStateDescriptor<>(
                "featureAuthSuccessRatioCliffState",
                TypeInformation.of(new TypeHint<CliffState>() {})
        );
        descriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        // ── NULL GUARD ──
        String authResult = txn.getAuthResult();
        if (authResult == null || authResult.trim().isEmpty()) {
            return;
        }

        // ── DEDUPLICATION ──
        CliffState currentState = state.value();
        if (currentState == null) {
            currentState = new CliffState();
        }

        String eventId = txn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) {
                return;
            }
            currentState.lastProcessedEventId = eventId;
        }

        long currentTs = resolveTimestamp(txn, ctx);
        currentState.lastEventTimestamp = currentTs;

        // ── RECORD RESULT ──
        boolean isSuccess = "Y".equalsIgnoreCase(authResult);
        currentState.resultHistory.addLast(isSuccess);
        if (isSuccess) {
            currentState.totalSuccessInHistory++;
        }

        // ── ENFORCE WINDOW SIZE ──
        if (currentState.resultHistory.size() > HISTORICAL_WINDOW) {
            boolean removed = currentState.resultHistory.removeFirst();
            if (removed) {
                currentState.totalSuccessInHistory--;
            }
        }

        // ── CLIFF EVALUATION (only when history is full) ──
        if (currentState.resultHistory.size() >= HISTORICAL_WINDOW) {
            // Calculate historical success rate (full window)
            double historicalRate = (double) currentState.totalSuccessInHistory / currentState.resultHistory.size();

            // Calculate recent success rate (last RECENT_WINDOW events)
            int recentSuccesses = 0;
            int idx = 0;
            int startIdx = currentState.resultHistory.size() - RECENT_WINDOW;
            for (boolean result : currentState.resultHistory) {
                if (idx >= startIdx && result) {
                    recentSuccesses++;
                }
                idx++;
            }
            double recentRate = (double) recentSuccesses / RECENT_WINDOW;

            // Calculate cliff
            double cliffSize = (historicalRate - recentRate) * 100.0; // Convert to percentage points

            if (cliffSize >= CLIFF_THRESHOLD && !currentState.cliffAlerted) {
                emitFeature(ctx.getCurrentKey(), historicalRate, recentRate, cliffSize, currentTs, out);
                currentState.cliffAlerted = true;
            } else if (cliffSize < RECOVERY_THRESHOLD && currentState.cliffAlerted) {
                // Rate has recovered — re-enable alerting
                currentState.cliffAlerted = false;
            }
        }

        state.update(currentState);
    }

    private long resolveTimestamp(InputMessageTxn txn, Context ctx) {
        long ts = ctx.timestamp();
        if (ts > 0) return ts;
        ts = txn.getKafkaSourceTimestamp();
        if (ts > 0) return ts;
        return System.currentTimeMillis();
    }

    private void emitFeature(String key, double historicalRate, double recentRate, double cliffSize,
                             long endTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_success_ratio_cliff");
        feature.setFeatureType("Percentage Points");
        feature.setFeatureValue(Math.round(cliffSize * 100.0) / 100.0); // 2 decimal places
        feature.setWindowEnd(endTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("historical_success_rate", Math.round(historicalRate * 10000.0) / 100.0 + "%");
        comments.put("recent_success_rate", Math.round(recentRate * 10000.0) / 100.0 + "%");
        comments.put("cliff_size_pct_points", Math.round(cliffSize * 100.0) / 100.0);
        comments.put("historical_window_size", HISTORICAL_WINDOW);
        comments.put("recent_window_size", RECENT_WINDOW);
        comments.put("cliff_threshold", CLIFF_THRESHOLD);
        comments.put("detection_timestamp", timestampToLocalDateTime(endTs).toString());
        comments.put("explanation", "Success rate dropped from "
                + Math.round(historicalRate * 10000.0) / 100.0 + "% (last " + HISTORICAL_WINDOW + " events) to "
                + Math.round(recentRate * 10000.0) / 100.0 + "% (last " + RECENT_WINDOW + " events) — a "
                + Math.round(cliffSize * 100.0) / 100.0 + " percentage point cliff indicating a fundamental "
                + "change in the identity's authentication behavior");

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }

        feature.setSkipComments(true);
        out.collect(feature);
    }

    private static LocalDateTime timestampToLocalDateTime(long timestamp) {
        return Instant.ofEpochMilli(timestamp).atZone(Driver.Configurations.istZone).toLocalDateTime();
    }
}
