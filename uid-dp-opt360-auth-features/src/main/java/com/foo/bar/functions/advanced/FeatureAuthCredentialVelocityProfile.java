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
 * Feature: AUTH CREDENTIAL VELOCITY PROFILE (Behavioral Tempo Fingerprint)
 * ════════════════════════════════════════════════════════════════════════════════
 *
 * WHAT IT DETECTS:
 *   This feature builds a PERSONALIZED statistical model of how frequently each
 *   identity (opt_id) authenticates. It learns the mean and standard deviation of
 *   inter-event time gaps using Welford's online algorithm (numerically stable,
 *   single-pass). After accumulating sufficient history (MIN_SAMPLES), it flags
 *   any event whose inter-event gap deviates by more than Z_THRESHOLD standard
 *   deviations from the learned mean.
 *
 * WHY IT MATTERS (Business Value):
 *   Every human has a natural "rhythm" — a field operator might authenticate every
 *   15-30 minutes during work hours. A bot or automated fraud tool operates at
 *   machine speed (milliseconds between attempts) or at suspiciously regular
 *   intervals. This feature catches:
 *     - Bot automation: gaps of milliseconds vs normal minutes
 *     - Credential sharing: sudden change in usage tempo
 *     - After-hours abuse: anomalous timing patterns
 *   Unlike simple velocity spike (which counts events/minute), this builds a TRUE
 *   statistical baseline PER IDENTITY and flags relative anomalies.
 *
 * MATHEMATICAL FOUNDATION — Welford's Online Algorithm:
 *   For each new gap x_n:
 *     count = count + 1
 *     delta = x_n - mean
 *     mean  = mean + delta / count
 *     delta2 = x_n - mean
 *     M2    = M2 + delta * delta2
 *     variance = M2 / (count - 1)   [Bessel's correction]
 *     stddev   = sqrt(variance)
 *     z_score  = (x_n - mean) / stddev
 *
 * EDGE CASES HANDLED:
 *   1. Out-of-order events: Skipped (would corrupt the temporal model)
 *   2. Kafka duplicates: eventId-based dedup
 *   3. Division by zero: Guarded when count < 2 or stddev == 0
 *   4. Negative gaps: Rejected (out-of-order event)
 *   5. Very first event: No gap to compute, just records timestamp
 *   6. Insufficient history: Requires MIN_SAMPLES (15) before evaluating
 *   7. Extreme outlier suppression: z-score is capped at 100 to avoid infinity
 *   8. Cool-down: After emitting, suppresses duplicate alerts for 60 seconds
 *
 * EMITTED FEATURE:
 *   - Feature Name : auth_credential_velocity_anomaly
 *   - Feature Value: z-score of the anomalous gap (higher = more anomalous)
 *   - Comments     : JSON with z_score, current_gap_sec, mean_gap_sec, stddev_gap_sec,
 *                    sample_count, direction (FASTER/SLOWER)
 */
@Slf4j
public class FeatureAuthCredentialVelocityProfile extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** Minimum number of inter-event gaps needed before we evaluate anomalies */
    private static final int MIN_SAMPLES = 15;

    /** Z-score threshold for anomaly detection (3 = 99.7% confidence) */
    private static final double Z_THRESHOLD = 3.0;

    /** Cool-down period after emitting an alert: 60 seconds */
    private static final long COOLDOWN_MS = 60_000L;

    /** Maximum z-score to report (prevents infinity from zero-variance edge) */
    private static final double MAX_Z_SCORE = 100.0;

    /**
     * State using Welford's online algorithm components.
     * All fields are public for Flink serialization.
     */
    public static class VelocityProfileState {
        /** Welford counters */
        public long count = 0;         // Number of gap samples
        public double mean = 0.0;      // Running mean of gaps (milliseconds)
        public double m2 = 0.0;        // Running sum of squared deviations

        /** Temporal tracking */
        public long lastEventTimestamp = -1L;
        public long lastEmissionTimestamp = 0L;

        /** Dedup */
        public String lastProcessedEventId = "";
    }

    private transient ValueState<VelocityProfileState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<VelocityProfileState> descriptor = new ValueStateDescriptor<>(
                "featureAuthCredentialVelocityProfileState",
                TypeInformation.of(new TypeHint<VelocityProfileState>() {})
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
        VelocityProfileState currentState = state.value();
        if (currentState == null) {
            currentState = new VelocityProfileState();
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

        // ── FIRST EVENT: Just record timestamp ──
        if (currentState.lastEventTimestamp < 0) {
            currentState.lastEventTimestamp = currentTs;
            state.update(currentState);
            return;
        }

        // ── OUT-OF-ORDER GUARD ──
        long gapMs = currentTs - currentState.lastEventTimestamp;
        if (gapMs < 0) {
            // Late-arriving event — do not corrupt the temporal model
            state.update(currentState);
            return;
        }

        // ── WELFORD'S ONLINE UPDATE ──
        currentState.count++;
        double delta = gapMs - currentState.mean;
        currentState.mean = currentState.mean + (delta / currentState.count);
        double delta2 = gapMs - currentState.mean;
        currentState.m2 = currentState.m2 + (delta * delta2);

        // ── ANOMALY EVALUATION ── (only after sufficient samples)
        if (currentState.count >= MIN_SAMPLES) {
            double variance = currentState.m2 / (currentState.count - 1); // Bessel's correction
            double stddev = Math.sqrt(variance);

            if (stddev > 0) {
                double zScore = Math.abs(gapMs - currentState.mean) / stddev;
                zScore = Math.min(zScore, MAX_Z_SCORE); // Cap to prevent infinity

                if (zScore >= Z_THRESHOLD) {
                    // ── COOL-DOWN CHECK ──
                    if (currentTs - currentState.lastEmissionTimestamp >= COOLDOWN_MS) {
                        String direction = gapMs < currentState.mean ? "FASTER" : "SLOWER";
                        emitFeature(ctx.getCurrentKey(), zScore, gapMs, currentState, direction, currentTs, out);
                        currentState.lastEmissionTimestamp = currentTs;
                    }
                }
            }
        }

        // ── UPDATE TEMPORAL ANCHOR ──
        currentState.lastEventTimestamp = currentTs;
        state.update(currentState);
    }

    private long resolveTimestamp(InputMessageTxn txn, Context ctx) {
        long ts = ctx.timestamp();
        if (ts > 0) return ts;
        ts = txn.getKafkaSourceTimestamp();
        if (ts > 0) return ts;
        return System.currentTimeMillis();
    }

    private void emitFeature(String key, double zScore, long currentGapMs, VelocityProfileState s, String direction, long endTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_credential_velocity_anomaly");
        feature.setFeatureType("Z-Score");
        feature.setFeatureValue(Math.round(zScore * 1000.0) / 1000.0); // 3 decimal places
        feature.setWindowEnd(endTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("z_score", Math.round(zScore * 1000.0) / 1000.0);
        comments.put("direction", direction);
        comments.put("current_gap_sec", Math.round((currentGapMs / 1000.0) * 100.0) / 100.0);
        comments.put("mean_gap_sec", Math.round((s.mean / 1000.0) * 100.0) / 100.0);
        double stddev = Math.sqrt(s.m2 / (s.count - 1));
        comments.put("stddev_gap_sec", Math.round((stddev / 1000.0) * 100.0) / 100.0);
        comments.put("sample_count", s.count);
        comments.put("explanation", direction.equals("FASTER")
                ? "Auth tempo is anomalously FAST compared to historical pattern — possible bot/automation"
                : "Auth tempo is anomalously SLOW compared to historical pattern — possible dormancy or credential sharing");

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }

        feature.setSkipComments(true);
        out.collect(feature);
    }
}
