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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * ════════════════════════════════════════════════════════════════════════════════
 * Feature: AUTH DORMANCY RESURRECTION
 * ════════════════════════════════════════════════════════════════════════════════
 *
 * WHAT IT DETECTS:
 *   An identity (opt_id) that has been COMPLETELY SILENT for an extended period
 *   (default: 7 days) and then suddenly reappears with a SUCCESSFUL authentication.
 *
 * WHY IT MATTERS (Business Value):
 *   This is one of the most reliable indicators of credential compromise. The attack
 *   pattern is: (1) Steal biometric data or credentials, (2) Wait days/weeks for
 *   monitoring attention to cool down, (3) Activate the stolen identity when nobody
 *   is watching. A legitimate user does not just vanish for a week and then pop back —
 *   their pattern is regular. This feature captures the "sleeper agent" attack vector.
 *
 * STATE DESIGN:
 *   - lastActivityTimestamp: Epoch ms of the most recent event (regardless of result)
 *   - lastProcessedEventId: Deduplication guard against Kafka redelivery
 *   - totalEvents: Count of all events processed (used for minimum history threshold)
 *
 * EDGE CASES HANDLED:
 *   1. Out-of-order events: Only updates lastActivityTimestamp if current > stored
 *   2. Kafka duplicates: Deduplicated via eventId tracking
 *   3. Null fields: Every field access is null-guarded
 *   4. Negative/zero timestamps: Falls back to System.currentTimeMillis()
 *   5. First event ever: No dormancy calculation on the very first event
 *   6. Minimum history: Requires at least 2 events before considering dormancy
 *   7. Dormancy after failure: Only emits on SUCCESS after dormancy (failure after
 *      dormancy is less suspicious — could be a scanner reboot)
 *
 * EMITTED FEATURE:
 *   - Feature Name : auth_dormancy_resurrection
 *   - Feature Value: Dormancy duration in HOURS (double)
 *   - Comments     : JSON with dormancy_hours, dormancy_days, last_seen_timestamp,
 *                    resurrection_timestamp, total_historical_events
 */
@Slf4j
public class FeatureAuthDormancyResurrection extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** Minimum dormancy gap to consider suspicious: 7 days in milliseconds */
    private static final long DORMANCY_THRESHOLD_MS = 7L * 24 * 60 * 60 * 1000;

    /** Minimum number of historical events before we start evaluating dormancy */
    private static final int MIN_HISTORY_EVENTS = 2;

    public static class DormancyState {
        public long lastActivityTimestamp = -1L;
        public String lastProcessedEventId = "";
        public int totalEvents = 0;
    }

    private transient ValueState<DormancyState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<DormancyState> descriptor = new ValueStateDescriptor<>(
                "featureAuthDormancyResurrectionState",
                TypeInformation.of(new TypeHint<DormancyState>() {})
        );
        descriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        // ── NULL GUARD: authResult is mandatory ──
        String authResult = txn.getAuthResult();
        if (authResult == null || authResult.trim().isEmpty()) {
            return;
        }

        // ── DEDUPLICATION via eventId ──
        DormancyState currentState = state.value();
        if (currentState == null) {
            currentState = new DormancyState();
        }

        String eventId = txn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) {
                return; // Exact Kafka redelivery — skip
            }
            currentState.lastProcessedEventId = eventId;
        }

        // ── TIMESTAMP RESOLUTION ──
        long currentTs = resolveTimestamp(txn, ctx);

        // ── OUT-OF-ORDER GUARD ──
        // If this event is older than our latest, we still process it for dedup
        // but do NOT update the lastActivityTimestamp (would corrupt dormancy calc)
        if (currentState.lastActivityTimestamp > 0 && currentTs < currentState.lastActivityTimestamp) {
            // Old event arrived late — acknowledge but do not evaluate dormancy
            state.update(currentState);
            return;
        }

        // ── DORMANCY EVALUATION ──
        if (currentState.lastActivityTimestamp > 0 && currentState.totalEvents >= MIN_HISTORY_EVENTS) {
            long dormancyMs = currentTs - currentState.lastActivityTimestamp;

            if (dormancyMs >= DORMANCY_THRESHOLD_MS && "Y".equalsIgnoreCase(authResult)) {
                // DORMANCY RESURRECTION DETECTED
                double dormancyHours = (double) dormancyMs / (1000.0 * 60.0 * 60.0);
                emitFeature(ctx.getCurrentKey(), dormancyHours, currentState.lastActivityTimestamp, currentTs, currentState.totalEvents, out);
                log.info("Dormancy resurrection detected for opt_id={}, dormancy_hours={}", ctx.getCurrentKey(), dormancyHours);
            }
        }

        // ── UPDATE STATE ──
        currentState.lastActivityTimestamp = currentTs;
        currentState.totalEvents++;
        state.update(currentState);
    }

    /**
     * Resolves the best available timestamp from the event.
     * Priority: ctx.timestamp() > kafkaSourceTimestamp > System.currentTimeMillis()
     */
    private long resolveTimestamp(InputMessageTxn txn, Context ctx) {
        long ts = ctx.timestamp();
        if (ts > 0) return ts;

        ts = txn.getKafkaSourceTimestamp();
        if (ts > 0) return ts;

        return System.currentTimeMillis();
    }

    private void emitFeature(String key, double dormancyHours, long lastSeenTs, long resurrectionTs, int historicalEvents, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_dormancy_resurrection");
        feature.setFeatureType("Duration (hours)");
        feature.setFeatureValue(Math.round(dormancyHours * 100.0) / 100.0); // 2 decimal places
        feature.setWindowStart(lastSeenTs);
        feature.setWindowEnd(resurrectionTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("dormancy_hours", Math.round(dormancyHours * 100.0) / 100.0);
        comments.put("dormancy_days", Math.round((dormancyHours / 24.0) * 100.0) / 100.0);
        comments.put("last_seen_timestamp", timestampToLocalDateTime(lastSeenTs).toString());
        comments.put("resurrection_timestamp", timestampToLocalDateTime(resurrectionTs).toString());
        comments.put("total_historical_events", historicalEvents);
        comments.put("threshold_days", DORMANCY_THRESHOLD_MS / (1000L * 60 * 60 * 24));

        try {
            String jsonString = objectMapper.writeValueAsString(comments);
            feature.setComments(jsonString);
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
