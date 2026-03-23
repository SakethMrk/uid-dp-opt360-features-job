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
 * Feature: AUTH AUA-DEVICE CORRELATION ANOMALY
 * ════════════════════════════════════════════════════════════════════════════════
 *
 * WHAT IT DETECTS:
 *   Builds a historical correlation matrix between AUAs (Authentication User
 *   Agencies) and DeviceCodes for each identity. After sufficient history is
 *   established, it detects when a COMPLETELY NEW (AUA, DeviceCode) pair appears
 *   that was NEVER seen before for this identity.
 *
 * WHY IT MATTERS (Business Value):
 *   In legitimate operations, an operator works at a FIXED agency (AUA) using
 *   a FIXED device (biometric scanner). The (AUA → Device) binding is very stable.
 *   When this binding BREAKS — i.e., a new AUA appears with a new device, or a
 *   known AUA suddenly uses a different device — it signals:
 *     - Credential theft: stolen identity used at a different agency on different hardware
 *     - Fraud syndicate activity: rotating identities across rented devices
 *     - Device cloning: spoofed deviceCodes mimicking stolen hardware
 *
 *   This is FAR MORE POWERFUL than simple device diversity or AUA diversity alone,
 *   because it captures the CROSS-DIMENSIONAL relationship.
 *
 * STATE DESIGN:
 *   - auaToDevices:  Map<AUA, Set<DeviceCode>> — which devices has this AUA used?
 *   - deviceToAuas:  Map<DeviceCode, Set<AUA>> — which AUAs have used this device?
 *   - totalPairs:    Total distinct (AUA, DeviceCode) pairs seen
 *   - totalEvents:   Total events processed (for minimum history threshold)
 *
 * EMISSION LOGIC:
 *   After MIN_EVENTS (10) events, if a brand-new (AUA, Device) pair appears:
 *     - Scenario A: Known AUA + New Device → "AUA_DEVICE_SHIFT" (the agency is using new hardware)
 *     - Scenario B: New AUA + Known Device → "DEVICE_AUA_SHIFT" (new agency hijacking existing hardware)
 *     - Scenario C: New AUA + New Device   → "FULL_CORRELATION_BREAK" (completely foreign combination)
 *
 * EDGE CASES HANDLED:
 *   1. Null AUA or DeviceCode: Silently skipped
 *   2. Kafka duplicates: eventId-based dedup
 *   3. State size bound: AUA and Device maps are each capped at 50 entries (LRU eviction)
 *   4. Out-of-order events: Accepted (correlation is order-independent)
 *   5. Empty strings: Treated as null
 */
@Slf4j
public class FeatureAuthAuaDeviceCorrelation extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** Minimum events before we start evaluating anomalies */
    private static final int MIN_EVENTS = 10;

    /** Maximum distinct AUAs or Devices to track per identity (memory safety) */
    private static final int MAX_MAP_SIZE = 50;

    public static class CorrelationState {
        /** AUA → set of device codes seen for that AUA */
        public Map<String, Set<String>> auaToDevices = new LinkedHashMap<>();
        /** DeviceCode → set of AUAs seen for that device */
        public Map<String, Set<String>> deviceToAuas = new LinkedHashMap<>();
        /** Total distinct (aua, device) pairs */
        public Set<String> knownPairs = new LinkedHashSet<>();
        public int totalEvents = 0;
        public String lastProcessedEventId = "";
    }

    private transient ValueState<CorrelationState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<CorrelationState> descriptor = new ValueStateDescriptor<>(
                "featureAuthAuaDeviceCorrelationState",
                TypeInformation.of(new TypeHint<CorrelationState>() {})
        );
        descriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        // ── NULL GUARDS ──
        String aua = txn.getAua();
        String deviceCode = txn.getDeviceCode();
        if (isBlank(aua) || isBlank(deviceCode)) {
            return;
        }

        // ── DEDUPLICATION ──
        CorrelationState currentState = state.value();
        if (currentState == null) {
            currentState = new CorrelationState();
        }

        String eventId = txn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) {
                return;
            }
            currentState.lastProcessedEventId = eventId;
        }

        currentState.totalEvents++;
        String pairKey = aua + "|" + deviceCode;

        // ── CHECK FOR ANOMALY (only after minimum history) ──
        if (currentState.totalEvents > MIN_EVENTS && !currentState.knownPairs.contains(pairKey)) {
            boolean auaKnown = currentState.auaToDevices.containsKey(aua);
            boolean deviceKnown = currentState.deviceToAuas.containsKey(deviceCode);

            String anomalyType;
            String explanation;

            if (auaKnown && !deviceKnown) {
                anomalyType = "AUA_DEVICE_SHIFT";
                explanation = "Known AUA '" + aua + "' is now using a BRAND NEW device '" + deviceCode + "' never seen before for this identity";
            } else if (!auaKnown && deviceKnown) {
                anomalyType = "DEVICE_AUA_SHIFT";
                explanation = "Known device '" + deviceCode + "' is now being accessed by a BRAND NEW AUA '" + aua + "' never seen before for this identity";
            } else if (!auaKnown && !deviceKnown) {
                anomalyType = "FULL_CORRELATION_BREAK";
                explanation = "Both AUA '" + aua + "' and device '" + deviceCode + "' are completely new — total foreign combination for this identity";
            } else {
                // Both known but this specific PAIR is new
                anomalyType = "NEW_PAIR_EXISTING_ENTITIES";
                explanation = "AUA '" + aua + "' and device '" + deviceCode + "' are individually known but have NEVER been used together before";
            }

            long currentTs = resolveTimestamp(txn, ctx);
            emitFeature(ctx.getCurrentKey(), anomalyType, explanation, aua, deviceCode, currentState, currentTs, out);
        }

        // ── UPDATE CORRELATION MATRIX ──
        currentState.knownPairs.add(pairKey);

        // AUA → Devices (with eviction)
        currentState.auaToDevices.computeIfAbsent(aua, k -> new LinkedHashSet<>()).add(deviceCode);
        evictIfNeeded(currentState.auaToDevices);

        // Device → AUAs (with eviction)
        currentState.deviceToAuas.computeIfAbsent(deviceCode, k -> new LinkedHashSet<>()).add(aua);
        evictIfNeeded(currentState.deviceToAuas);

        // Evict oldest pairs if too many
        if (currentState.knownPairs.size() > MAX_MAP_SIZE * MAX_MAP_SIZE) {
            Iterator<String> iter = currentState.knownPairs.iterator();
            if (iter.hasNext()) {
                iter.next();
                iter.remove();
            }
        }

        state.update(currentState);
    }

    private <V> void evictIfNeeded(Map<String, V> map) {
        while (map.size() > MAX_MAP_SIZE) {
            Iterator<String> iter = map.keySet().iterator();
            if (iter.hasNext()) {
                iter.next();
                iter.remove();
            }
        }
    }

    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private long resolveTimestamp(InputMessageTxn txn, Context ctx) {
        long ts = ctx.timestamp();
        if (ts > 0) return ts;
        ts = txn.getKafkaSourceTimestamp();
        if (ts > 0) return ts;
        return System.currentTimeMillis();
    }

    private void emitFeature(String key, String anomalyType, String explanation, String aua, String deviceCode,
                             CorrelationState s, long endTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_aua_device_correlation_break");
        feature.setFeatureType("Categorical");
        feature.setFeatureValue((double) s.knownPairs.size()); // Total known pairs as numeric signal
        feature.setWindowEnd(endTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("anomaly_type", anomalyType);
        comments.put("triggering_aua", aua);
        comments.put("triggering_device", deviceCode);
        comments.put("total_known_pairs", s.knownPairs.size());
        comments.put("distinct_auas_known", s.auaToDevices.size());
        comments.put("distinct_devices_known", s.deviceToAuas.size());
        comments.put("total_events_processed", s.totalEvents);
        comments.put("explanation", explanation);
        comments.put("detection_timestamp", timestampToLocalDateTime(endTs).toString());

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
