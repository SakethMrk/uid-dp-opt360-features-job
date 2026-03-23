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
 * Feature: AUTH PROGRESSIVE ERROR ESCALATION (Systematic Probing Detector)
 * ════════════════════════════════════════════════════════════════════════════════
 *
 * WHAT IT DETECTS:
 *   Identifies an identity whose CONSECUTIVE FAILED authentications produce a
 *   progressively DIVERSIFYING set of distinct error codes. Ordinary users fail
 *   with the SAME error repeatedly (e.g., dirty sensor → error 300, 300, 300).
 *   An attacker systematically probing the system will produce an ESCALATING
 *   pattern (300 → 330 → 510 → 720 → 810) as they try different attack vectors.
 *
 * WHY IT MATTERS (Business Value):
 *   This is the definitive "organized probing" detector. A legitimate user who
 *   fails auth does so because of one consistent reason (dirty sensor, wrong finger).
 *   Their error code is repetitive. An attacker — whether automated or manual —
 *   explores different failure modes: biometric mismatch, certificate issues, PID
 *   tampering, OTP bypass attempts. Each new attack vector produces a NEW error
 *   code. Seeing 4+ distinct error codes in consecutive failures is near-impossible
 *   for a real user but trivial for a prober.
 *
 * STATE DESIGN:
 *   - consecutiveFailures: List of <timestamp, errorCode, subErrorCode> in order
 *   - distinctErrorCodes: Set of unique error codes in the current streak
 *   - streakBroken: boolean flag tracking if a success resets the streak
 *
 * EMISSION LOGIC:
 *   Emits when:
 *     - MIN_DISTINCT_ERRORS (4) distinct error codes appear during a consecutive
 *       failure streak
 *     - The streak is bounded by SESSION_TIMEOUT_MS (30 minutes) — if failures
 *       span more than 30 minutes, the old failures are pruned
 *     - A success ("Y") resets the entire streak
 *
 * EDGE CASES HANDLED:
 *   1. Null errorCode/subErrorCode: Uses empty string as fallback (still tracked)
 *   2. Combined error tracking: Concatenates errorCode + ":" + subErrorCode for
 *      maximum granularity (two different sub-errors under the same parent count separately)
 *   3. Out-of-order events: Rejected (would corrupt the CONSECUTIVE requirement)
 *   4. Kafka duplicates: eventId-based dedup
 *   5. Session boundary: 30-minute timeout auto-resets (prevents cross-day contamination)
 *   6. Single-emission per streak: After emitting, clears the streak to prevent alert storms
 *   7. Success without prior failures: Gracefully ignored
 *
 * EMITTED FEATURE:
 *   - Feature Name : auth_progressive_error_escalation
 *   - Feature Value: Count of distinct error codes in the streak
 *   - Comments     : JSON with error_code_sequence, distinct_count, streak_duration_sec,
 *                    first_failure_time, last_failure_time
 */
@Slf4j
public class FeatureAuthProgressiveErrorEscalation extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** Minimum distinct error codes to trigger an emission */
    private static final int MIN_DISTINCT_ERRORS = 4;

    /** Maximum time window for a probing session: 30 minutes */
    private static final long SESSION_TIMEOUT_MS = 30L * 60 * 1000;

    public static class ErrorRecord {
        public long timestamp;
        public String errorComposite; // errorCode:subErrorCode

        public ErrorRecord() {}
        public ErrorRecord(long ts, String ec) {
            this.timestamp = ts;
            this.errorComposite = ec;
        }
    }

    public static class ProbingState {
        public LinkedList<ErrorRecord> consecutiveFailures = new LinkedList<>();
        public Set<String> distinctErrorCodes = new LinkedHashSet<>();
        public long streakStartTimestamp = 0L;
        public String lastProcessedEventId = "";
        public long lastEventTimestamp = -1L;
    }

    private transient ValueState<ProbingState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<ProbingState> descriptor = new ValueStateDescriptor<>(
                "featureAuthProgressiveErrorEscalationState",
                TypeInformation.of(new TypeHint<ProbingState>() {})
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
        ProbingState currentState = state.value();
        if (currentState == null) {
            currentState = new ProbingState();
        }

        String eventId = txn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) {
                return;
            }
            currentState.lastProcessedEventId = eventId;
        }

        // ── TIMESTAMP RESOLUTION ──
        long currentTs = resolveTimestamp(txn, ctx);

        // ── OUT-OF-ORDER GUARD ──
        if (currentState.lastEventTimestamp > 0 && currentTs < currentState.lastEventTimestamp) {
            state.update(currentState);
            return;
        }
        currentState.lastEventTimestamp = currentTs;

        // ── SUCCESS RESETS THE STREAK ──
        if ("Y".equalsIgnoreCase(authResult)) {
            if (!currentState.consecutiveFailures.isEmpty()) {
                // If the streak had enough distinct errors, emit before clearing
                if (currentState.distinctErrorCodes.size() >= MIN_DISTINCT_ERRORS) {
                    emitFeature(ctx.getCurrentKey(), currentState, currentTs, out);
                }
            }
            // Reset streak
            currentState.consecutiveFailures.clear();
            currentState.distinctErrorCodes.clear();
            currentState.streakStartTimestamp = 0L;
            state.update(currentState);
            return;
        }

        // ── FAILURE PROCESSING ──
        if ("N".equalsIgnoreCase(authResult)) {
            // Build composite error code
            String errorCode = txn.getErrorCode();
            String subErrorCode = txn.getSubErrorCode();
            String composite = (errorCode != null ? errorCode : "") + ":" + (subErrorCode != null ? subErrorCode : "");

            // Session timeout: prune old failures
            if (currentState.streakStartTimestamp > 0 && (currentTs - currentState.streakStartTimestamp) > SESSION_TIMEOUT_MS) {
                // Reset — this is a new probing session
                currentState.consecutiveFailures.clear();
                currentState.distinctErrorCodes.clear();
                currentState.streakStartTimestamp = 0L;
            }

            // Initialize streak
            if (currentState.consecutiveFailures.isEmpty()) {
                currentState.streakStartTimestamp = currentTs;
            }

            // Add failure record
            currentState.consecutiveFailures.addLast(new ErrorRecord(currentTs, composite));
            currentState.distinctErrorCodes.add(composite);

            // ── EMISSION CHECK ──
            if (currentState.distinctErrorCodes.size() >= MIN_DISTINCT_ERRORS) {
                emitFeature(ctx.getCurrentKey(), currentState, currentTs, out);

                // Clear to prevent alert storm (one alert per probing session)
                currentState.consecutiveFailures.clear();
                currentState.distinctErrorCodes.clear();
                currentState.streakStartTimestamp = 0L;
            }

            // Bound the failure list to prevent unbounded growth (max 100)
            while (currentState.consecutiveFailures.size() > 100) {
                ErrorRecord removed = currentState.consecutiveFailures.removeFirst();
                // Rebuild distinct set
                rebuildDistinctSet(currentState);
            }
        }

        state.update(currentState);
    }

    /**
     * Rebuilds the distinct error code set from the current failure list.
     * Called after eviction.
     */
    private void rebuildDistinctSet(ProbingState s) {
        s.distinctErrorCodes.clear();
        for (ErrorRecord rec : s.consecutiveFailures) {
            s.distinctErrorCodes.add(rec.errorComposite);
        }
    }

    private long resolveTimestamp(InputMessageTxn txn, Context ctx) {
        long ts = ctx.timestamp();
        if (ts > 0) return ts;
        ts = txn.getKafkaSourceTimestamp();
        if (ts > 0) return ts;
        return System.currentTimeMillis();
    }

    private void emitFeature(String key, ProbingState s, long endTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_progressive_error_escalation");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) s.distinctErrorCodes.size());
        feature.setWindowStart(s.streakStartTimestamp);
        feature.setWindowEnd(endTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();

        // Build ordered sequence of error codes
        List<String> sequence = new ArrayList<>();
        for (ErrorRecord rec : s.consecutiveFailures) {
            sequence.add(rec.errorComposite);
        }

        comments.put("error_code_sequence", sequence);
        comments.put("distinct_error_count", s.distinctErrorCodes.size());
        comments.put("distinct_errors", new ArrayList<>(s.distinctErrorCodes));
        comments.put("total_failures_in_streak", s.consecutiveFailures.size());
        long durationMs = endTs - s.streakStartTimestamp;
        comments.put("streak_duration_sec", Math.max(0, durationMs / 1000));
        comments.put("first_failure_time", timestampToLocalDateTime(s.streakStartTimestamp).toString());
        comments.put("last_failure_time", timestampToLocalDateTime(endTs).toString());
        comments.put("explanation", "Identity produced " + s.distinctErrorCodes.size()
                + " distinct error codes in " + s.consecutiveFailures.size()
                + " consecutive failures — indicative of systematic endpoint probing");

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
