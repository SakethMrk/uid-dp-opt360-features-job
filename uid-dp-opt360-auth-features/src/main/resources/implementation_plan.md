# Operator 360 — Feature Expansion & Kafka Refactoring Plan

## Background

The Operator 360 auth features job is a **Flink 1.17 streaming pipeline** that consumes authentication events from Kafka (`DE.OPT.AUTH_EVENTS.V1`) and computes real-time operator-level features. It publishes results to both **Kafka** (`DE.OPT.AUTH.FEATURES.TEST`) and **Iceberg/Hive** tables via dual sinks.

### Data Sources Available
| Stream | DTO | Key Fields |
|--------|-----|------------|
| **TXN** | [InputMessageTxn](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/InputMessageTxn.java#8-60) | `optId`, `authResult` (Y/N), `authCode`, `subErrorCode`, `authType`, `authDuration`, `deviceCode`, `modelId`, `locationStateCode`, `locationDistrictCode`, `serverId`, `certExpiryDate`, `fmrCount`, `firCount`, `faceUsed`, `fingerMatchScore`, `faceMatchScore`, etc. |
| **BIO** | [InputMessageBio](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/InputMessageBio.java#8-55) | `optId`, `bioType`, `matchScore`, `isLive`, `livenessRequestInitiatedTime`, `livenessResponseReceivedTime`, `faceLivenessScore`, `deepPrintIsMatched`, `deepPrintFIRMatchScore`, etc. |

### Existing Features (7 total)
| Feature | Type | Pattern |
|---------|------|---------|
| `auth_txn_count / failure_count / success_count` | Windowed Aggregation | `TumblingEventTimeWindows(60min)` |
| `auth_txn_gap_sec / failure_gap / success_gap` | Stateful KeyedProcess | Event-to-event time gap |
| `auth_txn_gap_sec_avg` | Stateful KeyedProcess | Rolling average of last 5 gaps |
| `auth_txn_oddhour_ratio` | Stateful KeyedProcess | Ratio of 8pm–8am txns, threshold-based |
| `auth_retry_attempts / duration_sec` | Stateful KeyedProcess | Failure→Success session detection |
| `auth_device_change` | Stateful KeyedProcess | Device code change detection |
| `auth_liveness_failure_streak_live_v1` | Stateful KeyedProcess | Consecutive 300-3 error streak |

---

## User Review Required

> [!IMPORTANT]
> **14 new features** are proposed below. Please review the list and let me know if you want to **add, remove, or reprioritize** any features before I start implementation.

> [!WARNING]
> The Kafka comments refactoring will change the `comments` field format from free-text to **JSON string** for liveness features. Downstream consumers should be notified.

---

## Part A: Kafka Comments JSON Refactoring

### Current Problem
In `FeatureLivenessStreakScore.emitFeature()`, comments are built as a raw `StringBuilder` with ad-hoc format:
```
 auth_code_initial:ABC,\nauth_code_initial_timestamp:2026-03-16T10:30:00,...
```
This is hard to parse downstream and inconsistent with structured Kafka messages.

### Proposed Fix
Build comments as a `LinkedHashMap<String, Object>`, serialize to JSON string using Jackson `ObjectMapper`, then set on `OutMessage.comments`.

#### [MODIFY] [FeatureLivenessStreakScore.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureLivenessStreakScore.java)
- Replace `StringBuilder` comments construction with `LinkedHashMap` → `objectMapper.writeValueAsString()`
- Add a `private static final ObjectMapper objectMapper = new ObjectMapper();`
- Comments will now be clean JSON like:
```json
{
  "auth_code_initial": "ABC123",
  "auth_code_initial_timestamp": "2026-03-16T10:30:00",
  "auth_code_final": "DEF456",
  "auth_code_final_timestamp": "2026-03-16T10:35:00",
  "device_codes": ["DEV001", "DEV002"],
  "model_id": "MDL100"
}
```

#### [MODIFY] [CommentsMapper.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/mapper/CommentsMapper.java)
- When `skipComments == true` and `comments != null`, keep the JSON string as-is (no overwrite)
- For non-skipComments features, also produce JSON-format comments using a map

---

## Part B: New Feature Game Plan — A to Z

### Wave 1: TXN-Based Features (6 features)

---

#### Feature 1: `auth_failure_rate_live_v1`
| Attribute | Detail |
|-----------|--------|
| **Business Use** | Detects operators with abnormally high failure ratios in real-time. A failure rate above 50% in a rolling window is a strong fraud/misuse signal |
| **Calculation** | `failure_count / total_count` computed over a sliding window of last N events (N=20). Emits when rate crosses thresholds: 0.3, 0.5, 0.7, 0.9 |
| **Flink Pattern** | `KeyedProcessFunction` with rolling counters |
| **Key By** | `optId` |
| **State** | `{totalCount, failureCount, highestEmittedThreshold}` |
| **TTL** | 48 hours |

#### [NEW] [FeatureAuthFailureRate.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthFailureRate.java)

---

#### Feature 2: `auth_velocity_spike_v1`
| Attribute | Detail |
|-----------|--------|
| **Business Use** | Catches sudden burst of transactions (e.g., bot/automated attacks). An operator doing 50+ txns in 1 minute is abnormal |
| **Calculation** | Track timestamps in a sliding 60-second window. Count events. If count > `SPIKE_THRESHOLD` (configurable, default 20), emit with the count as feature value |
| **Flink Pattern** | `KeyedProcessFunction` with timer-based cleanup |
| **Key By** | `optId` |
| **State** | `{List<Long> recentTimestamps, boolean spikeEmitted}` |
| **TTL** | 24 hours |

#### [NEW] [FeatureAuthVelocitySpike.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthVelocitySpike.java)

---

#### Feature 3: `auth_geo_anomaly_v1`
| Attribute | Detail |
|-----------|--------|
| **Business Use** | Detects when an operator's transactions originate from multiple different states/districts rapidly — possible device sharing or roaming fraud |
| **Calculation** | Track the `locationStateCode` and `locationDistrictCode` per operator. If a new state is seen that differs from the last known state within a 30-minute window, emit a feature with the number of distinct states as value |
| **Flink Pattern** | `KeyedProcessFunction` with `Set<String>` state |
| **Key By** | `optId` |
| **State** | `{Set<String> recentStates, Set<String> recentDistricts, long windowStart}` |
| **TTL** | 48 hours |

#### [NEW] [FeatureAuthGeoAnomaly.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthGeoAnomaly.java)

---

#### Feature 4: `auth_duration_outlier_v1`
| Attribute | Detail |
|-----------|--------|
| **Business Use** | Extremely fast (<100ms) or slow (>30s) auth durations indicate spoofing, network manipulation, or system issues per operator |
| **Calculation** | Parse `authDuration` (String → long ms). Maintain a running mean and variance (Welford's algorithm). If `|duration - mean| > 3 * stddev`, emit as outlier with z-score as value |
| **Flink Pattern** | `KeyedProcessFunction` with online statistics |
| **Key By** | `optId` |
| **State** | `{long count, double mean, double m2, long lastOutlierTimestamp}` |
| **TTL** | 48 hours |

#### [NEW] [FeatureAuthDurationOutlier.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthDurationOutlier.java)

---

#### Feature 5: `auth_modality_switch_v1`
| Attribute | Detail |
|-----------|--------|
| **Business Use** | Frequent switching between auth types (FP → Iris → Face → OTP) within a short time can indicate adversarial testing of biometric weaknesses |
| **Calculation** | Track last N auth types (`authType` field). If ≥3 distinct modalities used in last 10 events, emit with the count of distinct modalities |
| **Flink Pattern** | `KeyedProcessFunction` with circular buffer |
| **Key By** | `optId` |
| **State** | `{LinkedList<String> recentAuthTypes, boolean alerted}` |
| **TTL** | 48 hours |

#### [NEW] [FeatureAuthModalitySwitch.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthModalitySwitch.java)

---

#### Feature 6: `auth_error_code_hotspot_v1`
| Attribute | Detail |
|-----------|--------|
| **Business Use** | If a specific `subErrorCode` appears ≥N times in a rolling window, it signals a systematic issue with the operator (e.g., expired certs, bad devices, specific manipulation patterns) |
| **Calculation** | Maintain a `Map<String, Integer>` of subErrorCode → count in a sliding window. If any code exceeds threshold (default 5), emit with the code and count |
| **Flink Pattern** | `KeyedProcessFunction` |
| **Key By** | `optId` |
| **State** | `{Map<String, Integer> errorCodeCounts, long windowStart}` |
| **TTL** | 48 hours |

#### [NEW] [FeatureAuthErrorCodeHotspot.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthErrorCodeHotspot.java)

---

### Wave 2: BIO-Based Features (4 features)

> [!NOTE]
> The BIO stream (`inStream2`) is already consumed in the pipeline but **not used for any features yet**. These will be the first features leveraging biometric-level signals.

---

#### Feature 7: `bio_match_score_trend_v1`
| Attribute | Detail |
|-----------|--------|
| **Business Use** | A declining trend in biometric match scores indicates degrading biometric capture quality or potential spoofing attempts |
| **Calculation** | Maintain a sliding window of last 10 `matchScore` values. Compute slope via linear regression. If slope < `-THRESHOLD` (e.g., -2.0 per event), emit with slope as value |
| **Flink Pattern** | `KeyedProcessFunction` on BIO stream |
| **Key By** | `optId` |
| **State** | `{LinkedList<Double> recentScores, int eventCount}` |
| **TTL** | 48 hours |

#### [NEW] [FeatureBioMatchScoreTrend.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureBioMatchScoreTrend.java)

---

#### Feature 8: `bio_liveness_failure_streak_v1`
| Attribute | Detail |
|-----------|--------|
| **Business Use** | Consecutive liveness check failures (`isLive != "Y"`) at an operator indicate spoofing attempts or defective sensors |
| **Calculation** | Track consecutive failures. When streak is broken by a pass, emit streak count. Similar pattern to your [FeatureLivenessStreakScore](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureLivenessStreakScore.java#18-112) |
| **Flink Pattern** | `KeyedProcessFunction` on BIO stream |
| **Key By** | `optId` |
| **State** | `{int streakCount, long firstFailureTimestamp, long lastFailureTimestamp, String bioType}` |
| **TTL** | 24 hours |

#### [NEW] [FeatureBioLivenessFailureStreak.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureBioLivenessFailureStreak.java)

---

#### Feature 9: `bio_response_time_anomaly_v1`
| Attribute | Detail |
|-----------|--------|
| **Business Use** | Unusually fast liveness response times may indicate bypassed checks; unusually slow may indicate man-in-the-middle delays |
| **Calculation** | Parse `livenessRequestInitiatedTime` and `livenessResponseReceivedTime`, compute delta. Maintain running mean/stddev. Emit when z-score > 3 |
| **Flink Pattern** | `KeyedProcessFunction` on BIO stream |
| **Key By** | `optId` |
| **State** | `{long count, double mean, double m2}` (Welford's) |
| **TTL** | 48 hours |

#### [NEW] [FeatureBioResponseTimeAnomaly.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureBioResponseTimeAnomaly.java)

---

#### Feature 10: `bio_deepprint_anomaly_v1`
| Attribute | Detail |
|-----------|--------|
| **Business Use** | DeepPrint fingerprint matching failures or low scores at an operator may indicate fake-finger or presentation attacks |
| **Calculation** | Track `deepPrintIsMatched` failures and `deepPrintFIRMatchScore`. If failure rate > 30% in last 20 events, emit |
| **Flink Pattern** | `KeyedProcessFunction` on BIO stream |
| **Key By** | `optId` |
| **State** | `{int totalEvents, int failureEvents, LinkedList<Double> recentScores}` |
| **TTL** | 48 hours |

#### [NEW] [FeatureBioDeepPrintAnomaly.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureBioDeepPrintAnomaly.java)

---

### Wave 3: Cross-Stream & Advanced Features (4 features)

---

#### Feature 11: `auth_time_pattern_entropy_v1`
| Attribute | Detail |
|-----------|--------|
| **Business Use** | Measures how "random" an operator's transaction times are. Bots tend to be very regular (low entropy); genuine usage is moderate. Very high entropy across 24 hours also indicates suspicious automated distribution |
| **Calculation** | Bucket the hour-of-day (0-23) of each transaction. After every 50 events, compute Shannon entropy over the hour distribution. Normal operators: 2.5-3.5 bits. Bots: <1.5 or >4.0 |
| **Flink Pattern** | `KeyedProcessFunction` |
| **Key By** | `optId` |
| **State** | `{int[24] hourBuckets, int totalEvents}` |
| **TTL** | 48 hours |

#### [NEW] [FeatureAuthTimePatternEntropy.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthTimePatternEntropy.java)

---

#### Feature 12: `auth_aua_sa_diversity_v1`
| Attribute | Detail |
|-----------|--------|
| **Business Use** | Operators serving too many different AUA+SA combinations in a short period might be involved in identity fraud rings or unauthorized multi-party usage |
| **Calculation** | Track distinct [(aua, sa)](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/mapper/RowMapper.java#13-33) pairs in a rolling window. If count exceeds threshold (e.g., 10 distinct pairs in 1 hour), emit |
| **Flink Pattern** | `KeyedProcessFunction` |
| **Key By** | `optId` |
| **State** | `{Set<String> auaSaPairs, long windowStartTimestamp}` |
| **TTL** | 48 hours |

#### [NEW] [FeatureAuthAuaSaDiversity.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthAuaSaDiversity.java)

---

#### Feature 13: `auth_cert_expiry_proximity_v1`
| Attribute | Detail |
|-----------|--------|
| **Business Use** | Operators using devices with certificates about to expire need proactive alerts. Expired certificates cause auth failures at scale |
| **Calculation** | Parse `certExpiryDate`, compute days until expiry. Emit alerts at 30-day, 15-day, 7-day, and 1-day thresholds |
| **Flink Pattern** | `KeyedProcessFunction` |
| **Key By** | `optId` |
| **State** | `{int lastAlertedThresholdDays}` |
| **TTL** | 48 hours |

#### [NEW] [FeatureAuthCertExpiry.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthCertExpiry.java)

---

#### Feature 14: `auth_server_concentration_v1`
| Attribute | Detail |
|-----------|--------|
| **Business Use** | If an operator's transactions are abnormally concentrated on a single server (>80% of traffic), it might indicate routing manipulation or affinity attacks |
| **Calculation** | Maintain `Map<String, Integer>` of `serverId` → count. After every 20 events, compute the max share. If max share > 0.8, emit |
| **Flink Pattern** | `KeyedProcessFunction` |
| **Key By** | `optId` |
| **State** | `{Map<String, Integer> serverCounts, int totalCount}` |
| **TTL** | 48 hours |

#### [NEW] [FeatureAuthServerConcentration.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthServerConcentration.java)

---

## Part C: Infrastructure Changes

#### [MODIFY] [StateDescriptors.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/StateDescriptors.java)
- Add state descriptors for all 14 new features
- Each with appropriate TTL (24h or 48h as specified)

#### [MODIFY] [FlinkPipeline.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/pipeline/FlinkPipeline.java)
- Wire each new feature as a process/aggregate function on the appropriate stream
- BIO features (`FeatureBio*`) need a new [WatermarkStrategies](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/WatermarkStrategies.java#11-32) for [InputMessageBio](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/InputMessageBio.java#8-55) (or reuse existing)
- Bio stream features will need a `BioToOutMessage` adapter to emit [OutMessage](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/OutMessage.java#10-63)
- Union all new streams into the enriched output for both Kafka and Iceberg sinks

#### [NEW] [WatermarkStrategiesBio.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/WatermarkStrategiesBio.java)
- Watermark strategy for [InputMessageBio](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/InputMessageBio.java#8-55) using `eventTimestamp` field

---

## Verification Plan

### Automated (Compilation)
```bash
cd d:\Codebase\uid-dp-opt360-features-job\uid-dp-opt360-auth-features
mvn clean compile -DskipTests
```
This verifies all new classes compile without errors and integrate properly.

### Manual Verification
1. **Kafka Comments Format** — After deploying, consume from `DE.OPT.AUTH.FEATURES.TEST` and verify that the `comments` field for liveness features is valid JSON
2. **Feature Emission** — After deploying to a test environment, run the pipeline against a subset of live data and verify that new feature names appear in both Kafka and Iceberg outputs
3. **State TTL** — Verify that state is correctly cleaned up (no memory leak) by monitoring the Flink dashboard after 24-48 hours of runtime

> [!TIP]
> Since [TestFlinkPipeline.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/test/java/com/foo/bar/pipeline/TestFlinkPipeline.java) is currently empty, we could add unit tests for individual feature functions using Flink's `KeyedOneInputStreamOperatorTestHarness`. However, this requires additional test dependencies. Let me know if you'd like me to add test infrastructure as well.
