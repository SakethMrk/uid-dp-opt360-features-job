# Operator 360 Features (V2 Update)

This document covers the **10 newly injected features** that prioritize extreme simplicity, actionable intelligence, and uncompromised fault-tolerance. 

Every feature below operates purely on the `InputMessageTxn` stream and is built on a fault-tolerant architecture guarding against identical-timestamp Kafka duplicate events, state leaks, and memory bloat.

## 1. FeatureAuthGhostUptime
* **What It Does**: Tracks an operator who successfully or unsuccessfully authenticates continuously for **18 straight hours**, without ever taking a 30-minute break.
* **Why It’s Meaningful**: Humans need breaks and sleep. An 18-hour continuous runtime definitively flags a terminal being shared illegally across shifts or operated by a bot script.
* **Source Fields**: Event ID, Request DateTime.

## 2. FeatureAuthCrossBorderVelocity
* **What It Does**: Emits an alert if an operator authenticates from two different **Geographical State Codes** in under 2 hours.
* **Why It’s Meaningful**: Physical limits make this impossible without a plane trip. It instantly sniffs out stolen, shared biometric login creds operating on a VPN or proxy across multiple states simultaneously.
* **Source Fields**: Event ID, Location State Code, Event Timestamp.

## 3. FeatureAuthConcurrentAuaBurst
* **What It Does**: Captures transactions for **3 or more distinct AUAs (Agencies)** processing during the *exact same millisecond*.
* **Why It’s Meaningful**: A human operator physically inputs requests. Processing three completely separate clients simultaneously means the "operator" is actually an API server aggregating and routing stolen traffic in bulk.
* **Source Fields**: Event ID, AUA, Event Timestamp.

## 4. FeatureAuthBiometricReplay
* **What It Does**: Tracks the identical combination of (`fmrCount`, `firCount`, `fingerMatchScore`, `faceMatchScore`, `pidSize`). Alerts if the exact same blob is seen 3 times.
* **Why It’s Meaningful**: Biometric capture is analog; no two hashes are ever 100% identical down to the byte in timing, size, and minutiae count. Repeating this signature proves direct network replay.
* **Source Fields**: Event ID, fmrCount, firCount, fingerMatchScore, faceMatchScore, pidSize.

## 5. FeatureAuthHighRiskTimeWindow
* **What It Does**: Emits an alert if an operator accumulates 5 or more authentication failures purely between the hours of **1 AM and 4 AM IST**.
* **Why It’s Meaningful**: Failures happen during the day due to dirty sensors. Failures in the dead of night signify intentional spoofing attempts trying to avoid systemic or human oversight.
* **Source Fields**: Event ID, Auth Result, Request DateTime.

## 6. FeatureAuthFailureRecoverySpeed
* **What It Does**: Tracks an operator who fails 5+ times consecutively, and then immediately succeeds less than **1000 milliseconds (1 sec)** later.
* **Why It’s Meaningful**: Recovering from a bad biometric scan requires physically moving a finger or face, waiting for scanner light, and repositioning. A sub-second recovery means a script rapidly injected the correct hash after exhausting bad ones.
* **Source Fields**: Event ID, Auth Result, Request DateTime (converted to ms).

## 7. FeatureAuthRapidDeviceSwitch
* **What It Does**: Triggers if an operator uses **4 or more distinct physical devices** (`deviceCode`) in just a rolling 10-minute window.
* **Why It’s Meaningful**: Swapping an entire laptop or biometric machine takes time. Doing it 4 times in 10 minutes indicates a massive workstation farm or spoofed device identifiers overriding the physical machine logic.
* **Source Fields**: Event ID, Device Code, Request DateTime.

## 8. FeatureAuthMultiErrorSpray
* **What It Does**: Triggers if a single operator produces **8 distinct sub-error codes** in a 1-hour tumbling window.
* **Why It’s Meaningful**: Standard errors happen naturally (e.g., poor fingerprint quality). Causing 8 totally different, unrelated errors rapidly means the attacker is fuzz-testing the endpoint by manipulating payloads.
* **Source Fields**: Event ID, Sub Error Code, Request DateTime.

## 9. FeatureAuthNightSurge
* **What It Does**: Tracks the last 50+ volume of requests. Alerts if **>40% of the entire authentication volume** occurred specifically between 11 PM and 5 AM IST.
* **Why It’s Meaningful**: Unlike a one-off odd hour alert, this catches operators systematically doing half of their daily work at midnight, highlighting potentially unauthorized or illicit data entry outfits operating off the clock.
* **Source Fields**: Event ID, Request DateTime.

## 10. FeatureAuthConsecutiveIdenticalEvents
* **What It Does**: Alerts if the exact combination of (`AUA`, `SA`, `AuthType`, `ErrorCode`, `DeviceCode`, `PIDSize`, `FMRCount`) is identically duplicated 3 times in a row.
* **Why It’s Meaningful**: The broadest catching net for raw packet cloning. A duplicate packet doesn't just dupe the biometric hash; it dupes the entire transaction envelope blindly.
* **Source Fields**: Event ID, AUA, SA, Auth Type, Error Code, Device Code, PID Size, FMR Count.

---

### Key Technical Achievements
Each feature was built with a strict **Fault-Tolerance Matrix**:
1. **Idempotence**: Native `lastProcessedEventId` tracking ensures Kafka redelivery or duplicate timestamps cannot corrupt the mathematical models.
2. **Deterministic Checkpointing**: State TTL and Flink RocksDB structures explicitly ensure state can survive pod reboots indefinitely.
3. **Null Defenses**: Null checks universally applied before calculating mathematical operations prevents Job Manager NullPointerExceptions.
