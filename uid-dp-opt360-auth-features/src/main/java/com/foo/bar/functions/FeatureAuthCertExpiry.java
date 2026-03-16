package com.foo.bar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class FeatureAuthCertExpiry extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000L;

    public static class CertState {
        public String certId = "";
        public boolean alerted = false;
        public long lastTimestamp = 0;
    }

    private transient ValueState<CertState> state;
    private transient SimpleDateFormat sdf;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthCertExpiryDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction1Day());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthCertExpiryDescriptor);
        sdf = new SimpleDateFormat("yyyyMMdd"); // Assuming YYYYMMDD based on typical cert IDs
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String expiryDateStr = txn.getCertExpiryDate();
        if (expiryDateStr == null || expiryDateStr.trim().isEmpty()) return;

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        CertState currentState = state.value();
        if (currentState == null) currentState = new CertState();

        // Prevent spam: only alert once every 24 hours per operator
        if (!currentState.alerted || (currentTs - currentState.lastTimestamp > 24 * 60 * 60 * 1000L)) {
            currentState.lastTimestamp = currentTs;

            try {
                Date expiryDate;
                if (expiryDateStr.length() == 8) {
                    expiryDate = sdf.parse(expiryDateStr);
                } else {
                     // Attempt to fallback or parse full format, for this example we assume YYYYMMDD
                     String dateSub = expiryDateStr.substring(0, 8);
                     expiryDate = sdf.parse(dateSub);
                }
                
                long timeUntilExpiry = expiryDate.getTime() - currentTs;

                if (timeUntilExpiry > 0 && timeUntilExpiry <= SEVEN_DAYS_MS) {
                     emitFeature(ctx.getCurrentKey(), expiryDateStr, timeUntilExpiry, currentState, out);
                     currentState.alerted = true;
                }
            } catch (Exception e) {
                // Ignore format parsing issues
            }
        }

        state.update(currentState);
    }

    private void emitFeature(String key, String certId, long timeUntilExpiryMs, CertState state, Collector<OutMessage> out) {
        double daysUntilExpiry = timeUntilExpiryMs / (1000.0 * 60 * 60 * 24);

        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_cert_expiry_alert_v1");
        feature.setFeatureType("Days");
        feature.setFeatureValue(daysUntilExpiry);
        feature.setWindowEnd(state.lastTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("cert_id", certId);
        comments.put("days_remaining", String.format("%.1f", daysUntilExpiry));
        comments.put("alert", "Approaching expiration");

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
