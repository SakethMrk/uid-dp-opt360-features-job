# Operator 360 Feature Documentation

> **A Comprehensive A-to-Z Guide for Real-Time Security Features**
> This document serves as the master guide explaining the *business uses, security rationale, and Flink streaming implementations* behind all newly added features. It identifies anomalous behaviors at the Operator (`optId`) level.

---

## 🏗 Overview
The system processes streams of [InputMessageTxn](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/InputMessageTxn.java#8-60) utilizing Flink's `KeyedProcessFunction`s. All features are partitioned by `optId` and utilize Flink's native `ValueState` and Welford's algorithm to analyze risk in real-time, emitting insights to Kafka and Iceberg.

### 9 Functional Categories:
1. **[Data Harvesting](#1-data-harvesting-alerts)**
2. **[Burst & Bot Detection](#2-burst--bot-detection)**
3. **[Biometric Quality & Spoofing](#3-biometric-quality--spoofing)**
4. **[Device & Hardware Profiling](#4-device--hardware-profiling)**
5. **[Geographic & Location Masking](#5-geographic--location-masking)**
6. **[Rate & Ratio Tracking](#6-rate--ratio-tracking)**
7. **[Entity Diversification](#7-entity-diversification)**
8. **[Temporal Anomalies](#8-temporal-anomalies)**
9. **[Operational Hotspots](#9-operational-hotspots)**

---

## 1. Data Harvesting Alerts
*Finding scripts bulk-verifying stolen residency data.*

**1. `auth_uid_diversity_harvesting_v1`**
- **Use Case:** A malicious operator trying to authenticate tens of distinct residents (UID Tokens) rapidly. Standard demographic authentication shouldn't involve scanning a massive number of unique people at a single localized device in 1 hour.
- **Trigger:** > 50 distinct `uidToken` values processed within a tumbling 1-hour window.

**2. `auth_enrolment_hammering_v1`**
- **Use Case:** Identifies fake client loops rapidly submitting Enrolment packages.
- **Trigger:** ≥ 5 enrolment flags found within a tumbling window of 1 hour on a single operator.

---

## 2. Burst & Bot Detection
*Finding inhuman, scripted, automated authentication speed.*

**3. `auth_velocity_spike_v1`**
- **Use Case:** Detects high-velocity brute force or bot scripts firing requests continuously.
- **Trigger:** 15+ rapid transactions in a rolling tight 60-second window.

**4. `auth_failure_burst_v1`**
- **Use Case:** Operator trying to inject faulty payloads or performing dictionary attacks, leading to highly concentrated failures.
- **Trigger:** 8+ hard failure results (`authResult == N`) in a rolling 30-second window.

**5. `auth_modality_switch_v1`**
- **Use Case:** Frantic "hunt and peck" behavior — scripts attempting Fingerprint, failing, immediately switching to Face, failing, falling back to OTP.
- **Trigger:** 6+ modality changes within the last 10 requests.

---

## 3. Biometric Quality & Spoofing
*Identifying degradation in human biometric inputs, common via photo-of-photo or gum-finger attacks.*

**6. `auth_finger_score_decline_v1`**
- **Use Case:** Gummy finger degradation or poor scanner conditions.
- **Trigger:** Linear regression slope across the last 10 scores drops below `-1.5`.

**7. `auth_face_score_decline_v1`**
- **Use Case:** Adapting photo-of-photo injection where a mock image slowly worsens in quality match.
- **Trigger:** Linear regression slope across the last 10 scores drops below `-1.5`.

**8. `auth_iris_score_decline_v1`**
- **Use Case:** Similar to Face/Finger score degradation mapping.
- **Trigger:** Linear regression slope across the last 10 scores drops below `-1.5`.

**9. `auth_fmr_count_anomaly_v1`** (Finger Minutiae Variance)
- **Use Case:** When partial prints or totally different synthesized fingerprints are injected over time for the same UID.
- **Trigger:** Variance standard deviation > `15.0` over the last 10 recorded requests.

**10. `auth_pid_size_anomaly_v1`** (Payload Size Variance)
- **Use Case:** XML or Biometric packet injections often bloat or compress strangely relative to the operator's standard baseline.
- **Trigger:** Z-Score > `3.0` calculated via Welford's online variance algorithm.

---

## 4. Device & Hardware Profiling
*Catching unauthorized device sharing, virtualization, or tampering.*

**11. `auth_device_diversity_v1`**
- **Use Case:** An Operator ID should reasonably be tied to 1 or 2 devices. If multiple devices authenticate via one ID, credentials have been shared or compromised.
- **Trigger:** > 3 distinct `deviceCode`s in a 2-hour window.

**12. `auth_model_diversity_v1`**
- **Use Case:** Catching device emulators rotating device models.
- **Trigger:** > 2 distinct hardware `modelId`s in a 4-hour window.

**13. `auth_mi_diversity_v1`**
- **Use Case:** Like device diversity, but mapped strictly to the native Machine ID ([mi](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthVelocitySpike.java#87-110)), catching parallel OS footprints or Docker scaling attacks under one auth context.
- **Trigger:** > 3 distinct [mi](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthVelocitySpike.java#87-110)s in a 6-hour window.

**14. `auth_device_provider_switch_v1`**
- **Use Case:** A single operator violently bouncing between completely different sensor vendors (e.g., Mantra vs. SecuGen).
- **Trigger:** > 1 device provider in a 4-hour tumbling window.

**15. `auth_rd_software_change_v1`**
- **Use Case:** Catching malicious downgrade attacks or rogue tampering of the RD Service local client.
- **Trigger:** Tracking any stateful `registeredDeviceSoftwareVersion` and `registeredDeviceSoftwareId` changes.

---

## 5. Geographic & Location Masking
*Detecting VPNs, GPS spoofing, and disjointed residency queries.*

**16. `auth_geo_drift_v1`**
- **Use Case:** Distributed attacks using stolen proxies or operators physically moving impossibly fast between states and districts.
- **Trigger:** > 2 distinct `locationStateCode` or > 5 distinct `locationDistrictCode` in 1 hour.

**17. `auth_location_resident_mismatch_v1`**
- **Use Case:** Operator consistently authenticating residents mapped to fundamentally different native geographical profiles.
- **Trigger:** > 60% of rolling requests possess mis-matched authentication vs resident states.

---

## 6. Rate & Ratio Tracking
*Standard performance & authorization benchmarks.*

**18. `auth_failure_rate_rolling_v1`**
- **Use Case:** High failure rates across any given window indicates broken hardware or active brute-forcing. 
- **Trigger:** Dynamic tiered alerts exceeding 30%, 50%, 70%, 90% in a rolling size-50 queue.

**19. `auth_otp_fallback_ratio_v1`**
- **Use Case:** Operator bypassing biometrics consistently to utilize stolen devices or OTP SIM farms.
- **Trigger:** > 50% OTP reliance across the last 30 requests.

**20. `auth_success_without_bio_ratio_v1`**
- **Use Case:** Discovering authorization bypass loopholes granting positive auths with zero biometric inputs.
- **Trigger:** > 40% of positive transactions having no FMR, FIR, or Face presence.

---

## 7. Entity Diversification
*Monitoring fraud ring topologies spanning across multiple applications.*

**21. `auth_aua_sa_diversity_v1`**
- **Use Case:** A specific operator serving as an illegal mass-proxy, injecting transactions across numerous separate Sub-Agencies wildly.
- **Trigger:** Single operator generating requests into > 8 distinct Application (AUA + SA) tuples in 2 hours.

---

## 8. Temporal Anomalies
*Finding when transactions happen matching inhuman schedules.*

**22. `auth_time_entropy_bits_v1`**
- **Use Case:** Humans authenticate in bell-curves. Bots authenticate uniformly flat or precisely clustered.
- **Trigger:** Shannon Entropy bit-score is evaluated. (Bots: `< 1.5` or `> 4.2`).

**23. `auth_weekend_anomaly_v1`**
- **Use Case:** Operators typically take weekends off. If majority load happens directly on Saturday/Sunday, it could be rogue unsanctioned operations.
- **Trigger:** > 50% metric traffic load mapped entirely to weekends during a rolling 10-day evaluation.

---

## 9. Operational Hotspots
*Proactive maintenance to ensure stable deployments and catch MITM interception.*

**24. `auth_cert_expiry_alert_v1`**
- **Use Case:** Preventative downtime tracking.
- **Trigger:** Emits an alert proactively when a Public Certificate falls strictly under `7 Days` of remaining life.

**25. `auth_duration_outlier_v1`**
- **Use Case:** Network lag, server strain, or slow-man-in-the-middle interceptions hijacking transactions.
- **Trigger:** Online Welford's algorithm Z-Score > `3.0` calculating [(current time - requestInitiationTime)](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/mapper/RowMapperDevice.java#13-29).

**26. `auth_server_concentration_v1`**
- **Use Case:** If load balancing breaks and a single Environment handles 99% of an operator's requests, server failure risks amplify.
- **Trigger:** > 95% single-server reliance via a 1-hour tumbling window.

**27. `auth_error_code_hotspot_v1`**
- **Use Case:** Rather than a broad failure, this watches if an operator gets stuck in an identical precise Sub-Error Code loop consistently.
- **Trigger:** Single sub-error maps to > 80% volume of all errors.

---
*Generated by your loyal AI.*
