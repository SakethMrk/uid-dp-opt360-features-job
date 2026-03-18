package com.foo.bar.functions.bio;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageBio;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

public class FeatureBioMatchScoreTrend extends KeyedProcessFunction<String, InputMessageBio, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final double ALERT_SLOPE = -2.0;

    public static class ScoreState {
        public LinkedList<Double> scores = new LinkedList<>();
        public long lastTimestamp = 0;
        public boolean alerted = false;
    }

    private transient ValueState<ScoreState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureBioMatchScoreTrendDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureBioMatchScoreTrendDescriptor);
    }

    @Override
    public void processElement(InputMessageBio bio, Context ctx, Collector<OutMessage> out) throws Exception {
        String scoreStr = bio.getMatchScore();
        if (scoreStr == null || scoreStr.trim().isEmpty()) return;

        double score;
        try {
            score = Double.parseDouble(scoreStr.trim());
        } catch (NumberFormatException e) {
            return;
        }

        if (score <= 0) return;

        ScoreState currentState = state.value();
        if (currentState == null) currentState = new ScoreState();

        currentState.scores.addLast(score);
        if (currentState.scores.size() > 10) {
            currentState.scores.removeFirst();
        }

        currentState.lastTimestamp = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        if (currentState.scores.size() >= 10) {
            double slope = computeLinearRegressionSlope(currentState.scores);
            
            if (slope <= ALERT_SLOPE && !currentState.alerted) {
                emitFeature(ctx.getCurrentKey(), slope, currentState, out);
                currentState.alerted = true;
            } else if (slope > ALERT_SLOPE / 2) {
                currentState.alerted = false;
            }
        }

        state.update(currentState);
    }

    private double computeLinearRegressionSlope(LinkedList<Double> scores) {
        int n = scores.size();
        double sumX = 0, sumY = 0;
        for (int i = 0; i < n; i++) {
            sumX += i;
            sumY += scores.get(i);
        }
        double meanX = sumX / n;
        double meanY = sumY / n;

        double numerator = 0;
        double denominator = 0;
        for (int i = 0; i < n; i++) {
            numerator += (i - meanX) * (scores.get(i) - meanY);
            denominator += (i - meanX) * (i - meanX);
        }

        if (denominator == 0) return 0.0;
        return numerator / denominator;
    }

    private void emitFeature(String key, double slope, ScoreState state, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("bio_match_score_trend_v1");
        feature.setFeatureType("Trend");
        feature.setFeatureValue(slope);
        feature.setWindowEnd(state.lastTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("slope", slope);
        comments.put("recent_scores", state.scores);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
