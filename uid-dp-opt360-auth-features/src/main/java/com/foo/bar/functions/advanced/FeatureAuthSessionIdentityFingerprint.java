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
 * Feature: AUTH SESSION IDENTITY FINGERPRINT (Behavioral DNA)
 * ════════════════════════════════════════════════════════════════════════════════
 *
 * WHAT IT DETECTS:
 *   Creates a BEHAVIORAL FINGERPRINT for each identity by tracking the stable
 *   combination of (authType, modelId, deviceProviderId, registeredDeviceSoftwareId).
 *   This 4-tuple forms the identity's "DNA" — the hardware+software stack they
 *   consistently use. After establishing the DNA, it detects when ANY dimension
 *   of this tuple changes simultaneously.
 *
 * WHY IT MATTERS (Business Value):
 *   Individual field changes happen legitimately:
 *     - A device software update changes registeredDeviceSoftwareId
 *     - A new scanner changes modelId
 *     - Moving to OTP changes authType
 *   But when MULTIPLE dimensions change AT ONCE, it's extremely suspicious:
 *     - Different authType + different device model + different software =
 *       completely different person/machine using the same identity
 *     - This is the "fingerprint transplant" — a new operator trying to
 *       impersonate the original using entirely different equipment
 *
 *   This feature provides a SIMILARITY SCORE between the current transaction's
 *   fingerprint and the established baseline:
 *     - 4/4 match: Perfect match (normal)
 *     - 3/4 match: One change (likely legitimate)
 *     - 2/4 match: Two changes (SUSPICIOUS — emitted)
 *     - 1/4 or 0/4 match: Complete identity transplant (CRITICAL — emitted)
 *
 * STATE DESIGN:
 *   - fingerprint: Map<dimension_name, Map<value, count>> — frequency of each value per dimension
 *   - dominantFingerprint: The most common value for each dimension (derived)
 *   - totalEvents: Count for minimum history enforcement
 *
 * EMISSION THRESHOLD:
 *   - Emits when similarity score <= 2 out of 4 (50% or more of the fingerprint changed)
 *   - Cooldown: 5 minutes between alerts for the same identity
 *
 * EDGE CASES HANDLED:
 *   1. Null fields: Any null dimension is treated as "UNKNOWN" and still tracked
 *   2. Kafka duplicates: eventId-based dedup
 *   3. Insufficient history: Requires MIN_EVENTS (20) before evaluation
 *   4. Dominant value ties: Uses the alphabetically first value as tiebreaker
 *   5. State growth bound: Each dimension map capped at 30 distinct values
 *   6. Out-of-order events: Accepted (frequency table is order-independent)
 *   7. Cool-down: 5-minute suppress between emissions
 *   8. ALL dimensions missing: Skipped entirely (no fingerprint possible)
 *
 * EMITTED FEATURE:
 *   - Feature Name : auth_session_identity_fingerprint_break
 *   - Feature Value: Similarity score (0-4, lower = more anomalous)
 *   - Comments     : JSON with each dimension's baseline vs current value,
 *                    match details, similarity explanation
 */
@Slf4j
public class FeatureAuthSessionIdentityFingerprint extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** Dimensions that form the fingerprint */
    private static final String[] DIMENSIONS = {"authType", "modelId", "deviceProviderId", "rdSoftwareId"};

    /** Minimum events before evaluating */
    private static final int MIN_EVENTS = 20;

    /** Similarity threshold: emit when matches <= this value (out of 4) */
    private static final int MAX_SIMILARITY_FOR_ALERT = 2;

    /** Cooldown between alerts: 5 minutes */
    private static final long COOLDOWN_MS = 5L * 60 * 1000;

    /** Maximum distinct values per dimension */
    private static final int MAX_VALUES_PER_DIM = 30;

    public static class FingerprintState {
        /**
         * dimension_name → (value → count)
         * e.g., "authType" → {"FMR": 45, "OTP": 5}
         */
        public Map<String, Map<String, Integer>> dimensionFrequencies = new LinkedHashMap<>();
        public int totalEvents = 0;
        public long lastAlertTimestamp = 0L;
        public String lastProcessedEventId = "";
    }

    private transient ValueState<FingerprintState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<FingerprintState> descriptor = new ValueStateDescriptor<>(
                "featureAuthSessionIdentityFingerprintState",
                TypeInformation.of(new TypeHint<FingerprintState>() {})
        );
        descriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        // ── DEDUPLICATION ──
        FingerprintState currentState = state.value();
        if (currentState == null) {
            currentState = new FingerprintState();
        }

        String eventId = txn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) {
                return;
            }
            currentState.lastProcessedEventId = eventId;
        }

        // ── EXTRACT CURRENT FINGERPRINT ──
        Map<String, String> currentFingerprint = extractFingerprint(txn);

        // Check if ALL dimensions are null/unknown — skip entirely
        boolean allUnknown = true;
        for (String val : currentFingerprint.values()) {
            if (!"UNKNOWN".equals(val)) {
                allUnknown = false;
                break;
            }
        }
        if (allUnknown) {
            return;
        }

        long currentTs = resolveTimestamp(txn, ctx);

        // ── EVALUATE AGAINST BASELINE (before updating) ──
        if (currentState.totalEvents >= MIN_EVENTS) {
            Map<String, String> dominantFingerprint = getDominantFingerprint(currentState);
            int similarityScore = calculateSimilarity(currentFingerprint, dominantFingerprint);

            if (similarityScore <= MAX_SIMILARITY_FOR_ALERT) {
                // ── COOLDOWN CHECK ──
                if (currentTs - currentState.lastAlertTimestamp >= COOLDOWN_MS) {
                    emitFeature(ctx.getCurrentKey(), similarityScore, currentFingerprint,
                            dominantFingerprint, currentState, currentTs, out);
                    currentState.lastAlertTimestamp = currentTs;
                }
            }
        }

        // ── UPDATE FREQUENCY TABLES ──
        for (Map.Entry<String, String> entry : currentFingerprint.entrySet()) {
            Map<String, Integer> dimMap = currentState.dimensionFrequencies.computeIfAbsent(
                    entry.getKey(), k -> new LinkedHashMap<>());
            dimMap.merge(entry.getValue(), 1, Integer::sum);

            // Evict oldest if over limit
            while (dimMap.size() > MAX_VALUES_PER_DIM) {
                Iterator<String> iter = dimMap.keySet().iterator();
                if (iter.hasNext()) {
                    iter.next();
                    iter.remove();
                }
            }
        }
        currentState.totalEvents++;

        state.update(currentState);
    }

    /**
     * Extracts the 4-dimensional fingerprint from the transaction.
     */
    private Map<String, String> extractFingerprint(InputMessageTxn txn) {
        Map<String, String> fp = new LinkedHashMap<>();
        fp.put("authType", safeValue(txn.getAuthType()));
        fp.put("modelId", safeValue(txn.getModelId()));
        fp.put("deviceProviderId", safeValue(txn.getDeviceProviderId()));
        fp.put("rdSoftwareId", safeValue(txn.getRegisteredDeviceSoftwareId()));
        return fp;
    }

    /**
     * Derives the dominant (most frequent) value for each dimension.
     */
    private Map<String, String> getDominantFingerprint(FingerprintState s) {
        Map<String, String> dominant = new LinkedHashMap<>();
        for (String dim : DIMENSIONS) {
            Map<String, Integer> dimFreqs = s.dimensionFrequencies.get(dim);
            if (dimFreqs == null || dimFreqs.isEmpty()) {
                dominant.put(dim, "UNKNOWN");
            } else {
                String bestValue = null;
                int bestCount = -1;
                for (Map.Entry<String, Integer> entry : dimFreqs.entrySet()) {
                    if (entry.getValue() > bestCount ||
                            (entry.getValue() == bestCount && (bestValue == null || entry.getKey().compareTo(bestValue) < 0))) {
                        bestCount = entry.getValue();
                        bestValue = entry.getKey();
                    }
                }
                dominant.put(dim, bestValue != null ? bestValue : "UNKNOWN");
            }
        }
        return dominant;
    }

    /**
     * Calculates how many dimensions match between current and dominant fingerprint.
     * Returns 0-4 (number of matching dimensions).
     */
    private int calculateSimilarity(Map<String, String> current, Map<String, String> dominant) {
        int matches = 0;
        for (String dim : DIMENSIONS) {
            String currentVal = current.getOrDefault(dim, "UNKNOWN");
            String dominantVal = dominant.getOrDefault(dim, "UNKNOWN");
            if (currentVal.equals(dominantVal)) {
                matches++;
            }
        }
        return matches;
    }

    private String safeValue(String val) {
        return (val == null || val.trim().isEmpty()) ? "UNKNOWN" : val.trim();
    }

    private long resolveTimestamp(InputMessageTxn txn, Context ctx) {
        long ts = ctx.timestamp();
        if (ts > 0) return ts;
        ts = txn.getKafkaSourceTimestamp();
        if (ts > 0) return ts;
        return System.currentTimeMillis();
    }

    private void emitFeature(String key, int similarityScore, Map<String, String> current,
                             Map<String, String> dominant, FingerprintState s, long endTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_session_identity_fingerprint_break");
        feature.setFeatureType("Similarity Score");
        feature.setFeatureValue((double) similarityScore);
        feature.setWindowEnd(endTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("similarity_score", similarityScore + "/4");

        // Detail each dimension
        List<Map<String, String>> dimensionDetails = new ArrayList<>();
        for (String dim : DIMENSIONS) {
            Map<String, String> detail = new LinkedHashMap<>();
            detail.put("dimension", dim);
            detail.put("baseline_value", dominant.getOrDefault(dim, "UNKNOWN"));
            detail.put("current_value", current.getOrDefault(dim, "UNKNOWN"));
            detail.put("match", current.getOrDefault(dim, "UNKNOWN").equals(dominant.getOrDefault(dim, "UNKNOWN")) ? "YES" : "NO");
            dimensionDetails.add(detail);
        }
        comments.put("dimension_comparison", dimensionDetails);
        comments.put("total_events_in_baseline", s.totalEvents);
        comments.put("detection_timestamp", timestampToLocalDateTime(endTs).toString());

        String severity;
        if (similarityScore == 0) {
            severity = "CRITICAL — Complete identity transplant (0/4 dimensions match)";
        } else if (similarityScore == 1) {
            severity = "HIGH — Near-complete fingerprint change (1/4 dimensions match)";
        } else {
            severity = "MEDIUM — Significant fingerprint deviation (2/4 dimensions match)";
        }
        comments.put("severity", severity);

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
