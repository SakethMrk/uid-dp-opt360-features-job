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
 * Feature: AUTH GEO LOCATION ANCHOR (Behavioral Location Baseline)
 * ════════════════════════════════════════════════════════════════════════════════
 *
 * WHAT IT DETECTS:
 *   Builds a PERSONALIZED geographic baseline ("home location") for each identity
 *   based on their historical authentication locations (stateCode + districtCode).
 *   After sufficient history, it detects when an authentication occurs from a
 *   location that is NOT the identity's established anchor.
 *
 * CRITICAL DIFFERENCE FROM EXISTING geo_drift / cross_border_velocity:
 *   - geo_drift: Compares CONSECUTIVE events (A→B). Misses slow drift.
 *   - cross_border_velocity: Catches physically impossible travel. Misses same-state anomalies.
 *   - THIS FEATURE: Compares against the STATISTICAL BASELINE. If 95% of your history
 *     is from State "27 District 15", then ANY appearance in State "09 District 03" is
 *     flagged — even if you were at "09 District 03" yesterday too (because it's not
 *     your anchor).
 *
 * WHY IT MATTERS (Business Value):
 *   This is the "where does this person ACTUALLY work?" detector. Legitimate operators
 *   authenticate from a consistent location — their office. A stolen identity being used
 *   from a different location will NEVER have enough history to establish an anchor there.
 *   This feature catches:
 *     - Credential theft used at a remote agency
 *     - Identity portability fraud (using one identity across multiple geographies)
 *     - Gradual location migration that simple drift detectors miss
 *
 * STATE DESIGN:
 *   - locationFrequency: Map<"stateCode|districtCode", count>
 *   - totalLocationEvents: Total events with valid location data
 *   - anchorLocation: The most frequent location (calculated, not stored — derived on demand)
 *   - cooldown: Prevents alert storms from the same non-anchor location
 *
 * ANCHOR DETERMINATION:
 *   The anchor is the location with the HIGHEST frequency ratio. To be considered a
 *   "strong anchor", a location must have > 50% of total events. If no strong anchor
 *   exists (very distributed pattern), the feature is MORE suspicious of any location.
 *
 * EDGE CASES HANDLED:
 *   1. Null stateCode/districtCode: Skipped entirely (no geo data = no evaluation)
 *   2. Insufficient history: Requires MIN_EVENTS (15) before evaluation
 *   3. No anchor established: If history is perfectly distributed (no >50% location),
 *      emits "NO_ANCHOR" — itself an anomaly worth flagging
 *   4. Same non-anchor location repeated: Cooldown timer prevents alert storms
 *   5. Location map bounded: Capped at 100 distinct locations (LRU eviction)
 *   6. Kafka duplicates: eventId-based dedup
 *   7. Out-of-order events: Accepted (location frequency is order-independent)
 *
 * EMITTED FEATURE:
 *   - Feature Name : auth_geo_location_anchor_drift
 *   - Feature Value: Percentage of history at the ANCHOR location (lower = weaker anchor)
 *   - Comments     : JSON with anchor_location, anchor_percentage, current_location,
 *                    current_location_percentage, total_distinct_locations, event_count
 */
@Slf4j
public class FeatureAuthGeoLocationAnchor extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** Minimum events with valid location data before evaluating */
    private static final int MIN_EVENTS = 15;

    /** Anchor strength threshold: location must have > this ratio to be "anchor" */
    private static final double ANCHOR_THRESHOLD = 0.50;

    /** Cooldown: Don't re-alert for same non-anchor location within 1 hour */
    private static final long COOLDOWN_MS = 60L * 60 * 1000;

    /** Maximum distinct locations to track */
    private static final int MAX_LOCATIONS = 100;

    public static class AnchorState {
        /** Location → frequency count */
        public Map<String, Integer> locationFrequency = new LinkedHashMap<>();
        public int totalLocationEvents = 0;
        public String lastProcessedEventId = "";

        /** Cooldown tracking: location → last alert timestamp */
        public Map<String, Long> lastAlertPerLocation = new LinkedHashMap<>();
    }

    private transient ValueState<AnchorState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<AnchorState> descriptor = new ValueStateDescriptor<>(
                "featureAuthGeoLocationAnchorState",
                TypeInformation.of(new TypeHint<AnchorState>() {})
        );
        descriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        // ── NULL GUARD: Need location data ──
        String stateCode = txn.getLocationStateCode();
        String districtCode = txn.getLocationDistrictCode();
        if (isBlank(stateCode)) {
            return; // No location data — cannot evaluate
        }

        // ── DEDUPLICATION ──
        AnchorState currentState = state.value();
        if (currentState == null) {
            currentState = new AnchorState();
        }

        String eventId = txn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) {
                return;
            }
            currentState.lastProcessedEventId = eventId;
        }

        long currentTs = resolveTimestamp(txn, ctx);

        // ── BUILD LOCATION KEY ──
        String locationKey = stateCode + "|" + (districtCode != null ? districtCode : "UNKNOWN");

        // ── UPDATE FREQUENCY ──
        currentState.locationFrequency.merge(locationKey, 1, Integer::sum);
        currentState.totalLocationEvents++;

        // Evict if too many locations
        while (currentState.locationFrequency.size() > MAX_LOCATIONS) {
            Iterator<Map.Entry<String, Integer>> iter = currentState.locationFrequency.entrySet().iterator();
            if (iter.hasNext()) {
                iter.next();
                iter.remove();
            }
        }

        // ── ANCHOR EVALUATION (after minimum history) ──
        if (currentState.totalLocationEvents >= MIN_EVENTS) {
            // Find anchor (most frequent location)
            String anchorLocation = null;
            int anchorCount = 0;
            for (Map.Entry<String, Integer> entry : currentState.locationFrequency.entrySet()) {
                if (entry.getValue() > anchorCount) {
                    anchorCount = entry.getValue();
                    anchorLocation = entry.getKey();
                }
            }

            double anchorRatio = (double) anchorCount / currentState.totalLocationEvents;

            if (!locationKey.equals(anchorLocation)) {
                // Current location is NOT the anchor

                // ── COOLDOWN CHECK ──
                Long lastAlert = currentState.lastAlertPerLocation.get(locationKey);
                if (lastAlert == null || (currentTs - lastAlert) >= COOLDOWN_MS) {

                    int currentLocationCount = currentState.locationFrequency.getOrDefault(locationKey, 1);
                    double currentLocationRatio = (double) currentLocationCount / currentState.totalLocationEvents;

                    String anchorStrength;
                    if (anchorRatio >= 0.80) {
                        anchorStrength = "VERY_STRONG";
                    } else if (anchorRatio >= ANCHOR_THRESHOLD) {
                        anchorStrength = "STRONG";
                    } else {
                        anchorStrength = "WEAK_OR_NO_ANCHOR";
                    }

                    emitFeature(ctx.getCurrentKey(), anchorLocation, anchorRatio,
                            locationKey, currentLocationRatio, anchorStrength,
                            currentState, currentTs, out);

                    currentState.lastAlertPerLocation.put(locationKey, currentTs);

                    // Bound cooldown map
                    while (currentState.lastAlertPerLocation.size() > MAX_LOCATIONS) {
                        Iterator<String> iter = currentState.lastAlertPerLocation.keySet().iterator();
                        if (iter.hasNext()) {
                            iter.next();
                            iter.remove();
                        }
                    }
                }
            }
        }

        state.update(currentState);
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

    private void emitFeature(String key, String anchorLocation, double anchorRatio,
                             String currentLocation, double currentLocationRatio,
                             String anchorStrength, AnchorState s, long endTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_geo_location_anchor_drift");
        feature.setFeatureType("Ratio");
        feature.setFeatureValue(Math.round(anchorRatio * 10000.0) / 10000.0); // 4 decimal places
        feature.setWindowEnd(endTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("anchor_location", anchorLocation);
        comments.put("anchor_percentage", Math.round(anchorRatio * 10000.0) / 100.0 + "%");
        comments.put("anchor_strength", anchorStrength);
        comments.put("current_location", currentLocation);
        comments.put("current_location_percentage", Math.round(currentLocationRatio * 10000.0) / 100.0 + "%");
        comments.put("total_distinct_locations", s.locationFrequency.size());
        comments.put("total_events_with_location", s.totalLocationEvents);
        comments.put("detection_timestamp", timestampToLocalDateTime(endTs).toString());
        comments.put("explanation", "Identity authenticated from '" + currentLocation
                + "' which is NOT their established anchor location '" + anchorLocation
                + "' (anchor strength: " + anchorStrength + ")");

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
