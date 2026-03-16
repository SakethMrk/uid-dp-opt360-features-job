# Operator 360 — Complete TXN Feature Game Plan (A → Z)

> Every feature below uses [InputMessageTxn](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/InputMessageTxn.java#8-60) fields. Keyed by `optId`. All are `KeyedProcessFunction` unless stated otherwise.

---

## ✅ Already Implemented (7 features)

| # | Feature Name | Uses Fields |
|---|-------------|-------------|
| E1 | `auth_txn_count / failure_count / success_count` | `authResult` |
| E2 | `auth_txn_gap_sec` | `reqDateTime` |
| E3 | `auth_txn_gap_sec_avg` | `reqDateTime` |
| E4 | `auth_txn_oddhour_ratio` | `reqDateTime` (hour) |
| E5 | `auth_retry_attempts / duration_sec` | `authResult` |
| E6 | `auth_device_change` | `deviceCode` |
| E7 | `auth_liveness_failure_streak_live_v1` | `subErrorCode(300-3)`, `deviceCode`, `authCode`, `modelId` |

---

## 🆕 New TXN Features to Implement (20 features)

### Category A — Rate & Ratio Features

#### A1. `auth_failure_rate_rolling_v1`
| | |
|---|---|
| **Why** | Operators with abnormally high failure ratios signal fraud/device issues. Unlike the windowed count, this is a **rolling per-event** metric |
| **Logic** | Maintain `totalCount` and `failureCount` across last `N=50` events (circular buffer). Compute `ratio = failureCount/totalCount`. Emit when ratio crosses thresholds: **0.30, 0.50, 0.70, 0.90** |
| **Fields** | `authResult` |
| **State** | `{LinkedList<Boolean> recentResults, int failureCount, double lastEmittedThreshold}` |
| **Emits** | Feature value = ratio (0.0–1.0) |
| **TTL** | 48h |

#### A2. `auth_otp_fallback_ratio_v1`
| | |
|---|---|
| **Why** | Operators excessively using OTP instead of biometric auth may indicate broken devices or bypassing biometric security |
| **Logic** | Track last 30 auth types. If `authType == "O"` (OTP) ratio exceeds **0.5**, emit. Normal operators should be <0.2 OTP |
| **Fields** | `authType` |
| **State** | `{LinkedList<String> recentAuthTypes, int otpCount}` |
| **Emits** | Feature value = OTP ratio |
| **TTL** | 48h |

#### A3. `auth_success_without_bio_ratio_v1`
| | |
|---|---|
| **Why** | Successful auths where `fmrCount=0`, `firCount=0`, and `faceUsed=N` suggest non-biometric approvals — potential policy bypass |
| **Logic** | In a rolling window of 30 events, count successes where all biometric fields are absent/zero. If ratio > 0.4, emit |
| **Fields** | `authResult`, `fmrCount`, `firCount`, `faceUsed` |
| **State** | `{int totalSuccess, int suspiciousSuccess, LinkedList<Boolean> buffer}` |
| **Emits** | Feature value = suspicious ratio |
| **TTL** | 48h |

---

### Category B — Velocity & Burst Detection

#### B1. `auth_velocity_spike_v1`
| | |
|---|---|
| **Why** | Sudden burst of transactions (bot attacks, automated scripts). Normal operators do 1-5 txns/min; bots do 20+ |
| **Logic** | Keep timestamps in a 60-second sliding window (prune expired). If count > `SPIKE_THRESHOLD=15`, emit. Use Flink timer to clean up window after 60s of inactivity |
| **Fields** | `reqDateTime` (event time) |
| **State** | `{LinkedList<Long> timestamps, boolean spikeAlerted}` |
| **Emits** | Feature value = events-per-minute count |
| **TTL** | 24h |

#### B2. `auth_failure_burst_v1`
| | |
|---|---|
| **Why** | A concentrated burst of failures (e.g., 10+ failures in 30 seconds) is different from a high failure rate — it indicates active attack or systematic probing |
| **Logic** | Track failure timestamps only. Keep 30-second window. If >= 8 failures in window, emit |
| **Fields** | `authResult`, event timestamp |
| **State** | `{LinkedList<Long> failureTimestamps, boolean burstAlerted}` |
| **Emits** | Feature value = failures-per-30s |
| **TTL** | 24h |

---

### Category C — Geographic & Location Features

#### C1. `auth_geo_drift_v1`
| | |
|---|---|
| **Why** | If an operator's transactions originate from multiple states/districts within a short window, it signals device sharing or mobile fraud rings |
| **Logic** | Track distinct `locationStateCode` values in a 1-hour tumbling window. If > 2 distinct states, emit. Also track district-level — if > 5 distinct districts, emit separately |
| **Fields** | `locationStateCode`, `locationDistrictCode` |
| **State** | `{Set<String> states, Set<String> districts, long windowStart}` |
| **Emits** | Feature value = count of distinct states (or districts) |
| **TTL** | 48h |

#### C2. `auth_location_resident_mismatch_v1`
| | |
|---|---|
| **Why** | When `locationStateCode != residentStateCode` consistently, it means auths are happening away from the resident's home state — possible identity theft or migrant exploitation |
| **Logic** | In rolling 20 events, count where `locationStateCode != residentStateCode` (both non-null). If mismatch ratio > 0.6, emit |
| **Fields** | `locationStateCode`, `residentStateCode`, `locationDistrictCode`, `residentDistrictCode` |
| **State** | `{int totalEvents, int mismatchEvents, LinkedList<Boolean> buffer}` |
| **Emits** | Feature value = mismatch ratio |
| **TTL** | 48h |

---

### Category D — Device & Hardware Features

#### D1. `auth_device_diversity_v1`
| | |
|---|---|
| **Why** | Legitimate operators use 1-2 devices. An operator using 5+ distinct devices in a short period is suspicious (device cycling/sharing) |
| **Logic** | Track distinct `deviceCode` values in a 2-hour window. If count > 3, emit |
| **Fields** | `deviceCode` |
| **State** | `{Set<String> devices, long windowStart}` |
| **Emits** | Feature value = distinct device count |
| **TTL** | 48h |

#### D2. `auth_model_diversity_v1`
| | |
|---|---|
| **Why** | Multiple device models at a single operator station within hours is abnormal — indicates device swapping or tampering |
| **Logic** | Track distinct `modelId` in a 4-hour window. If > 2 distinct models, emit |
| **Fields** | `modelId` |
| **State** | `{Set<String> models, long windowStart}` |
| **Emits** | Feature value = distinct model count |
| **TTL** | 48h |

#### D3. `auth_rd_software_change_v1`
| | |
|---|---|
| **Why** | Changing Registered Device software ID or version mid-session can indicate tampering. Legitimate devices don't change RD software frequently |
| **Logic** | Track the last seen [(registeredDeviceSoftwareId, registeredDeviceSoftwareVersion)](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/mapper/RowMapper.java#13-33). If it changes, emit with previous & new version in comments |
| **Fields** | `registeredDeviceSoftwareId`, `registeredDeviceSoftwareVersion` |
| **State** | `{String lastSoftwareId, String lastSoftwareVersion, int changeCount}` |
| **Emits** | Feature value = total change count |
| **TTL** | 48h |

#### D4. `auth_device_provider_switch_v1`
| | |
|---|---|
| **Why** | Switching between different device providers at the same operator is unusual — it may indicate unauthorized hardware or shared infrastructure |
| **Logic** | Track distinct `deviceProviderId` values. If > 1 provider seen in a 4-hour window, emit |
| **Fields** | `deviceProviderId` |
| **State** | `{Set<String> providers, long windowStart}` |
| **Emits** | Feature value = distinct provider count |
| **TTL** | 48h |

---

### Category E — Biometric Quality Features (from TXN stream)

#### E1. `auth_finger_score_decline_v1`
| | |
|---|---|
| **Why** | Declining finger match scores over time suggest sensor degradation, dirty sensors, or spoofing with deteriorating fake fingers |
| **Logic** | Collect last 10 `fingerMatchScore` values (parse String → double). Compute simple linear regression slope. If slope < -1.5 per event, emit |
| **Fields** | `fingerMatchScore` |
| **State** | `{LinkedList<Double> scores}` |
| **Emits** | Feature value = slope |
| **TTL** | 48h |

#### E2. `auth_face_score_decline_v1`
| | |
|---|---|
| **Why** | Same as fingerprint but for face — declining face match scores indicate photo aging, lighting manipulation, or printed-photo attacks |
| **Logic** | Same slope detection on last 10 `faceMatchScore` values |
| **Fields** | `faceMatchScore` |
| **State** | `{LinkedList<Double> scores}` |
| **Emits** | Feature value = slope |
| **TTL** | 48h |

#### E3. `auth_fmr_count_anomaly_v1`
| | |
|---|---|
| **Why** | The `fmrCount` (fingerprint minutiae count) should be fairly consistent for real fingers. Very low counts (<10) suggest partial/fake fingerprints. Very high variance suggests multiple different fingers or synthetic prints |
| **Logic** | Track last 10 `fmrCount` values. Compute stddev. If stddev > threshold (e.g., 15), emit |
| **Fields** | `fmrCount` |
| **State** | `{LinkedList<Integer> counts}` |
| **Emits** | Feature value = stddev |
| **TTL** | 48h |

#### E4. `auth_pid_size_anomaly_v1`
| | |
|---|---|
| **Why** | `pidSize` (PID packet size) should be relatively consistent for same auth type. Unusually small PIDs might be crafted/empty packets; unusually large ones might carry injected data |
| **Logic** | Track mean and stddev of `pidSize` using Welford's. If `|current - mean| > 3σ`, emit |
| **Fields** | `pidSize` |
| **State** | `{long count, double mean, double m2}` |
| **Emits** | Feature value = z-score |
| **TTL** | 48h |

---

### Category F — Temporal Pattern Features

#### F1. `auth_time_entropy_v1`
| | |
|---|---|
| **Why** | Bots have very regular patterns (low entropy); normal humans are moderate. Shannon entropy of transaction hour-of-day distribution is a powerful profiling tool |
| **Logic** | Bucket each event into hour (0-23). After every 50 events, compute Shannon entropy: `H = -Σ p(h) * log2(p(h))`. Normal: 2.5-3.5 bits. Suspicious: <1.5 (bot) or >4.2 (spread-attack) |
| **Fields** | event timestamp → extract hour |
| **State** | `{int[24] hourBuckets, int totalEvents}` |
| **Emits** | Feature value = entropy in bits |
| **TTL** | 48h |

#### F2. `auth_weekend_anomaly_v1`
| | |
|---|---|
| **Why** | Most legitimate operators have reduced weekend activity. High weekend-to-weekday ratio suggests automated usage |
| **Logic** | Track `weekdayCount` and `weekendCount` over a rolling window (reset weekly). Saturdays = 6, Sundays = 7. If `weekendRatio > 0.5` after 20+ events, emit |
| **Fields** | event timestamp → extract day of week |
| **State** | `{int weekdayCount, int weekendCount, long cycleStart}` |
| **Emits** | Feature value = weekend ratio |
| **TTL** | 48h |

---

### Category G — Identity & Entity Features

#### G1. `auth_aua_sa_diversity_v1`
| | |
|---|---|
| **Why** | A legitimate operator typically serves 1-3 AUA+SA combinations. Many distinct pairs suggest the operator is being used across multiple agencies (identity fraud ring) |
| **Logic** | Track distinct `aua + ":" + sa` string pairs in a 2-hour window. If > 8 distinct, emit |
| **Fields** | `aua`, [sa](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/OutMessage.java#10-63) |
| **State** | `{Set<String> pairs, long windowStart}` |
| **Emits** | Feature value = distinct pair count |
| **TTL** | 48h |

#### G2. `auth_enrolment_hammering_v1`
| | |
|---|---|
| **Why** | The same `enrolmentReferenceId` being authenticated repeatedly at the same operator is suspicious — could be testing stolen identity documents |
| **Logic** | Maintain a `Map<String, Integer>` of `enrolmentReferenceId → count` in a 1-hour window. If any enrolment exceeds 5 attempts, emit |
| **Fields** | `enrolmentReferenceId` |
| **State** | `{Map<String, Integer> enrolmentCounts, long windowStart}` |
| **Emits** | Feature value = max attempts for any single enrolment |
| **TTL** | 48h |

---

### Category H — Infrastructure & System Features

#### H1. `auth_duration_outlier_v1`
| | |
|---|---|
| **Why** | Extremely fast auths (<50ms) suggest bypassed verification. Extremely slow (>30s) suggest MITM delays or network manipulation |
| **Logic** | Parse `authDuration` → long ms. Welford's online mean/variance. If z-score > 3.0, emit |
| **Fields** | `authDuration` |
| **State** | `{long count, double mean, double m2}` |
| **Emits** | Feature value = z-score |
| **TTL** | 48h |

#### H2. `auth_server_concentration_v1`
| | |
|---|---|
| **Why** | If 80%+ of an operator's traffic goes to a single `serverId`, it could indicate routing manipulation or server affinity attacks |
| **Logic** | Maintain `Map<String, Integer>` of `serverId → count`. After every 25 events, compute HHI (Herfindahl index) = `Σ (share_i)²`. If HHI > 0.64 (one server > 80%), emit |
| **Fields** | `serverId` |
| **State** | `{Map<String, Integer> serverCounts, int totalCount}` |
| **Emits** | Feature value = HHI |
| **TTL** | 48h |

#### H3. `auth_cert_expiry_alert_v1`
| | |
|---|---|
| **Why** | Proactive alerting before device certificates expire. Expired certs = mass auth failures |
| **Logic** | Parse `certExpiryDate` → compute days until expiry. Emit alert at thresholds: **30, 15, 7, 3, 1** days |
| **Fields** | `certExpiryDate` |
| **State** | `{int lastAlertedThreshold}` |
| **Emits** | Feature value = days until expiry |
| **TTL** | 48h |

#### H4. `auth_error_code_hotspot_v1`
| | |
|---|---|
| **Why** | Clustering of specific `subErrorCode` values at an operator signals systematic issues (e.g., `300-3` = liveness, `998` = cancelled, etc.) |
| **Logic** | Track `Map<String, Integer>` of `subErrorCode → count` in a 1-hour window. If any code exceeds 5 occurrences OR dominates >60% of errors, emit |
| **Fields** | `subErrorCode` |
| **State** | `{Map<String, Integer> errorCounts, int totalErrors, long windowStart}` |
| **Emits** | Feature value = count of dominant error code |
| **TTL** | 48h |

#### H5. `auth_modality_switch_v1`
| | |
|---|---|
| **Why** | Rapid switching between FP/Iris/Face/OTP modalities suggests adversarial probing of biometric weaknesses |
| **Logic** | Track last 10 `authType` values. Count distinct types. If ≥ 3 distinct in 10 events, emit |
| **Fields** | `authType` |
| **State** | `{LinkedList<String> recentAuthTypes}` |
| **Emits** | Feature value = distinct modality count |
| **TTL** | 48h |

---

## Summary Matrix

| # | Feature | Category | Risk Signal | Input Fields |
|---|---------|----------|-------------|--------------|
| A1 | Failure Rate Rolling | Rate | High failure ratio | `authResult` |
| A2 | OTP Fallback Ratio | Rate | Over-reliance on OTP | `authType` |
| A3 | Success Without Bio | Rate | Non-biometric approvals | `authResult`, `fmrCount`, `firCount`, `faceUsed` |
| B1 | Velocity Spike | Burst | Bot/automation | timestamp |
| B2 | Failure Burst | Burst | Active attack | `authResult`, timestamp |
| C1 | Geo Drift | Location | Device sharing | `locationStateCode/DistrictCode` |
| C2 | Location-Resident Mismatch | Location | Identity theft | `location*`, `resident*` |
| D1 | Device Diversity | Device | Device cycling | `deviceCode` |
| D2 | Model Diversity | Device | Device swapping | `modelId` |
| D3 | RD Software Change | Device | Tampering | `registeredDeviceSoftware*` |
| D4 | Device Provider Switch | Device | Unauthorized HW | `deviceProviderId` |
| E1 | Finger Score Decline | Bio Quality | Sensor/spoofing | `fingerMatchScore` |
| E2 | Face Score Decline | Bio Quality | Photo attack | `faceMatchScore` |
| E3 | FMR Count Anomaly | Bio Quality | Fake fingerprint | `fmrCount` |
| E4 | PID Size Anomaly | Bio Quality | Packet manipulation | `pidSize` |
| F1 | Time Entropy | Temporal | Bot detection | timestamp |
| F2 | Weekend Anomaly | Temporal | Automated usage | timestamp |
| G1 | AUA/SA Diversity | Entity | Fraud ring | `aua`, [sa](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/OutMessage.java#10-63) |
| G2 | Enrolment Hammering | Entity | Stolen identity | `enrolmentReferenceId` |
| H1 | Duration Outlier | Infra | MITM/bypass | `authDuration` |
| H2 | Server Concentration | Infra | Routing attack | `serverId` |
| H3 | Cert Expiry Alert | Infra | Proactive ops | `certExpiryDate` |
| H4 | Error Code Hotspot | Infra | Systematic issue | `subErrorCode` |
| H5 | Modality Switch | Infra | Adversarial probe | `authType` |
