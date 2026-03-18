# Operator 360 Auth Features Job

Welcome to the **Operator 360** feature extraction project! This Flink-based data engineering pipeline processes real-time transaction streams to analyze, track, and score the behavior of authentication operators. 

By analyzing the `InputMessageTxn` stream, the pipeline aggregates data over sliding and tumbling time windows to identify anomalies, potential fraud, spoofing attacks, and system issues, ultimately creating a comprehensive "360-degree" profile of every operator in the system.

## 🚀 What Has Happened So Far?

In this project, we have completely transformed the observability and security logic for Operator Authentication:
1. **Kafka JSON Comments Refactoring**: Upgraded the existing event reporting (like Liveness Streak) to publish rich, structured JSON strings to the downstream Kafka sink instead of chaotic strings.
2. **Stateful Feature Pipeline**: Implemented 24 robust `KeyedProcessFunction` stream operators to calculate real-time analytics.
3. **Strict Data Dependency**: Eliminated all "assumed" fields. The analytics are calculated entirely using the existing, native dataset limits (e.g., `serverId`, `authDuration`, `enrolmentReferenceId`, `certExpiryDate`).
4. **Resilient Windowing**: Brought in rolling sequence tracking, tumbling windows, and offline Welford's Variance algorithms to keep state footprints low while doing heavy math.
5. **Pipeline Integration**: Successfully wired all 24 new feature streams into a massive unified DataStream pipeline that sinks to `OutMessage`.

---

## 🛡️ Feature Breakdown (The 24 Security Guards)

Below is the detailed explanation for every real-time feature we've built, identifying irregular trends per-operator.

### 1. FeatureAuthFailureRate
* **Goal**: Track rolling transaction failure severity for a given operator.
* **Use Case**: An operator trying to guess authentication details or operating completely broken biometric hardware.
* **Result**: Emits an alert if the failure rate (`authResult == N`) exceeds 30%, 50%, 70%, or 90% over the last 50 transactions.
* **Impact**: Rapidly isolates broken enrollment centers or active brute-force attacks.

### 2. FeatureAuthOtpFallbackRatio
* **Goal**: Identify operators who consistently avoid biometric authentication.
* **Use Case**: Operators using stolen resident details and bypassing biometrics by spamming OTPs.
* **Result**: Triggers when >50% of an operator's last 30 transactions rely strictly on OTP.
* **Impact**: Plugs a critical security loophole where malicious users ignore physical presence checks.

### 3. FeatureAuthSuccessWithoutBio
* **Goal**: Find successful transactions that lack biometric presence.
* **Use Case**: Finding operators who have discovered system exploits allowing them to generate "Success" authentications without scanning a finger, face, or iris.
* **Result**: Triggers if >40% of positive transactions in the last 100 lack biometric parameters.
* **Impact**: Ensures 100% compliance with strict biometric presence requirements.

### 4. FeatureAuthVelocitySpike
* **Goal**: Detect superhuman authentication speed.
* **Use Case**: Scripts and bots submitting bulk requests automatically.
* **Result**: Triggers if an operator exceeds 15 transactions within a tight 60-second window.
* **Impact**: Instantly halts automated replay attacks or dictionary spam.

### 5. FeatureAuthFailureBurst
* **Goal**: Identify highly concentrated, rapid authentication rejections.
* **Use Case**: A bot attempting multiple rapid injections of bad biometric hashes.
* **Result**: Triggers when 8 or more failures occur within 30 seconds.
* **Impact**: Catches algorithmic spoofing attempts before they find a bypassing hash weakness.

### 6. FeatureAuthGeoDrift
* **Goal**: Ensure operators are not teleporting.
* **Use Case**: Hackers sharing operator credentials across different states simultaneously via VPNs.
* **Result**: Alerts if an operator submits transactions from > 2 states or > 5 districts in a single hour.
* **Impact**: Neutralizes proxy-hopping and credential sharing.

### 7. FeatureAuthLocationResidentMismatch
* **Goal**: Compare operator location with the resident's native address.
* **Use Case**: Catching operators exclusively processing residents who live thousands of miles away.
* **Result**: Alerts if >60% of requests in a rolling 100-request window occur in a state different than the resident's state.
* **Impact**: Detects black-market demographic harvesting operations.

### 8. FeatureAuthDeviceDiversity
* **Goal**: Limit the physical hardware footprint of a single operator.
* **Use Case**: An operator sharing their login across the office to 5 different unregistered laptops.
* **Result**: Triggers if more than 3 unique `deviceCode`s are seen for one operator in 2 hours.
* **Impact**: Enforces 1-to-1 hardware mapping for security accountability.

### 9. FeatureAuthModelDiversity
* **Goal**: Detect device emulators masking their identities.
* **Use Case**: Hackers using Android Studio or virtual machines, rapidly changing the simulated `modelId` to hide.
* **Result**: Triggers if more than 2 distinct hardware models are authenticated by a single operator in 4 hours.
* **Impact**: Destroys virtualized emulator fraud rings.

### 10. FeatureAuthRdSoftwareChange
* **Goal**: Monitor the software health of the Registered Device (RD) client.
* **Use Case**: An operator downgrading their application software to exploit a known vulnerability in an older version.
* **Result**: Alerts instantly if the `registeredDeviceSoftwareVersion` drastically changes mid-shift.
* **Impact**: Maintains strict compliance checking for up-to-date client software.

### 11. FeatureAuthDeviceProviderSwitch
* **Goal**: Monitor biometric sensor vendors.
* **Use Case**: An operator desperately swapping between Mantra, SECUGEN, and Morpho sensors because one is blocking their fake gummy fingers.
* **Result**: Alerts if > 1 device provider is logged in a 4-hour window by the same operator.
* **Impact**: Highlights potential physical hardware tampering.

### 12. FeatureAuthFingerScoreDecline
* **Goal**: Track fingerprint degradation trends.
* **Use Case**: A fake glue fingerprint melting away or flattening out after being pressed against the scanner 10 times.
* **Result**: Uses linear regression mapping. Triggers if the `fingerMatchScore` trendline drops below a slope of -1.5.
* **Impact**: Detects physical spoofing properties over sequential requests.

### 13. FeatureAuthFaceScoreDecline
* **Goal**: Identify photographic spoofing degradation.
* **Use Case**: Glare, wrinkles, or blur slowly accumulating while trying to scan a printed photograph of a resident.
* **Result**: Triggers if the `faceMatchScore` linear regression slope hits -1.5.
* **Impact**: Halts continuous brute-force attacks using static imagery.

### 14. FeatureAuthFmrCountAnomaly
* **Goal**: Track fingerprint pattern volatility.
* **Use Case**: An operator injecting entirely different synthetic `fmrCount` minutiae payloads on the same resident's UID.
* **Result**: Calculates Standard Deviation. Alerts if variance > 15.0 over 10 requests.
* **Impact**: Stops advanced algorithmic hash injections.

### 15. FeatureAuthPidSizeAnomaly
* **Goal**: Measure data packet "bloat".
* **Use Case**: Malware or hidden payloads attached to the XML/JSON PID blocks.
* **Result**: Calculates Z-Score via Welford's algorithm. Alerts if PID Size Z-Score > 3.0.
* **Impact**: Flags potentially compromised network traffic or bloated spoof packets.

### 16. FeatureAuthTimeEntropy
* **Goal**: Find mathematical perfection in transaction timing.
* **Use Case**: Scripts dispatching a request exactly every 5500 milliseconds.
* **Result**: Uses Shannon Entropy. Alerts if bit-score falls out of human ranges (< 1.5 or > 4.2).
* **Impact**: Defeats sophisticated automated bot traffic.

### 17. FeatureAuthWeekendAnomaly
* **Goal**: Track unapproved operational hours.
* **Use Case**: Rogue employees accessing the system heavily on Sunday mornings to avoid oversight.
* **Result**: Alerts if >50% of the operator's rolling history occurs exclusively on weekends.
* **Impact**: Enforces standard business operating hour compliance.

### 18. FeatureAuthAuaSaDiversity
* **Goal**: Detect agency harvesting.
* **Use Case**: A single operator acting as an open proxy, querying 9 different government/bank Sub Agencies simultaneously.
* **Result**: Triggers if > 8 distinct AUA/SA combinations are hit in 2 hours.
* **Impact**: Isolates compromised terminals serving multiple black-market clients.

### 19. FeatureAuthEnrolmentHammering
* **Goal**: Stop mass fake resident generation.
* **Use Case**: Bots heavily submitting new `enrolmentReferenceId` payloads back-to-back.
* **Result**: Alerts if an operator submits 5+ distinct enrollments in 1 hour.
* **Impact**: Protects the database from synthetic identity generation spam.

### 20. FeatureAuthDurationOutlier
* **Goal**: Identify latency injections.
* **Use Case**: A "Man in the Middle" (MITM) attacker intercepting the payload, editing it, and forwarding it, resulting in massive delay.
* **Result**: Uses Welford Z-Score tracking. Alerts if the `authDuration` is an extreme outlier (Z-Score > 3.0).
* **Impact**: Flags slow-routing or intercepted network topologies.

### 21. FeatureAuthServerConcentration
* **Goal**: Verify load balancer integrity.
* **Use Case**: Hackers targeting one specific weak `serverId` environment intentionally.
* **Result**: Alerts if >95% of traffic over 50 requests successfully hits only one server instance.
* **Impact**: Reveals broken network routing or targeted infrastructure probing.

### 22. FeatureAuthCertExpiry
* **Goal**: Proactively maintain up-time.
* **Use Case**: Identifying soon-to-expire vendor or app certificates before they bring down the node.
* **Result**: Warns system admins if `certExpiryDate` proves there are 7 days or less remaining on the cert.
* **Impact**: Prevents costly system outages.

### 23. FeatureAuthErrorCodeHotspot
* **Goal**: Identify repetitive programmatic blocking.
* **Use Case**: A bot stuck in a loop generating the exact same `subErrorCode` endlessly.
* **Result**: Alerts if a single error code covers >80% of an operator's minimum 20 error requests in an hour.
* **Impact**: Highly effective at catching unmonitored scripts failing repeatedly.

### 24. FeatureAuthModalitySwitch
* **Goal**: Catch desperate authentication "Hunt and Peck" methods.
* **Use Case**: Fraudsters trying fingerprint... failing... trying face... failing... trying OTP... 
* **Result**: Alerts if the operator switches their `authType` 6 or more times within 10 rolling requests.
* **Impact**: Isolates terminal locations housing stolen identity data.

### 25. FeatureBioMatchScoreTrend
* **Goal**: Track long-term biometric match confidence degradation.
* **Use Case**: Repeatedly scanning a deteriorating gummy finger or a faded printed photograph over time.
* **Result**: Emits an alert if the continuous `matchScore` linear regression slope drops below -2.0 over 10 consecutive bio scans.
* **Impact**: Halts advanced physical spoofing techniques that pass initial basic checks.

### 26. FeatureBioLivenessFailureStreak
* **Goal**: Isolate severe liveliness spoof rejections.
* **Use Case**: An operator trying repeatedly to bypass 3D depth cameras or active liveliness checks using 2D media.
* **Result**: Starts counting consecutive `isLive = N` fails. Emits an event the moment a success breaks the long streak, pinpointing the start and end of the attack window.
* **Impact**: Identifies exactly when an operator was testing spoofing boundaries.

### 27. FeatureBioResponseTimeAnomaly
* **Goal**: Detect bypassed or intercepted biometric hardware.
* **Use Case**: Malware skipping the camera scan entirely (0ms response) or routing the image to an external server for processing (massive delay).
* **Result**: Analyzes the gap between `livenessRequestInitiatedTime` and `livenessResponseReceivedTime`. Emits alert if variance Z-Score > 3.0.
* **Impact**: Protects against hardware-level bypasses and man-in-the-middle software injections.

### 28. FeatureBioDeepPrintAnomaly
* **Goal**: Monitor the DeepPrint machine-learning AI model failure rate.
* **Use Case**: An operator exclusively scanning unusual, damaged, or synthetic fingerprints that the DeepPrint model struggles to map.
* **Result**: Alerts if `deepPrintIsMatched` failures exceed a 30% rolling rate over 20 scanning attempts.
* **Impact**: Acts as a secondary defense layer tracking AI-level confidence drops.

### 29. FeatureBioFaceLivenessScoreDecline
* **Goal**: Track declining facial spoof-defense margins.
* **Use Case**: Increasing glare, blur, or wrinkling as an operator holds up a printed mask or phone screen displaying a face.
* **Result**: Emits if the `faceLivenessScore` linear regression slope falls beneath -1.5 over 8 scans.
* **Impact**: Detects continuous brute-forcing of facial presentation attacks.

### 30. FeatureBioModalityConcentration
* **Goal**: Ensure diverse capture modality health.
* **Use Case**: An operator exclusively running Iris scanners because their fingerprint device is compromised but unreported.
* **Result**: Analyzes the incoming `bioType`. Triggers if a single bioType covers > 95% of a 30-request window.
* **Impact**: Highlights broken physical hardware or exploited specific-modality pathways.

### 31. FeatureBioImageSizeAnomaly
* **Goal**: Filter out synthetic or deeply compressed payloads.
* **Use Case**: An attacker injecting massive, uncompressed high-resolution images manually, or tiny compressed thumbnails that couldn't have come from the authorized camera.
* **Result**: Uses Welford's algorithm on `imageSize`. Alerts if Z-Score variance > 3.0.
* **Impact**: Immediately stops payload injection loops bypassing the authorized scanner interface.

### 32. FeatureBioProbeMinutiaeAnomaly
* **Goal**: Discover wildly inconsistent fingerprint structures.
* **Use Case**: An operator attempting to combine half of one person's fingerprint with half of another's, or shifting a fake finger resulting in massive detail changes.
* **Result**: Emits an alert if the Standard Deviation of the `probeMinutiae` count exceeds 15.0 over 10 requests.
* **Impact**: Neutralizes complex synthetic minutiae manipulations.

---

### 33. FeatureAuthGhostUptime
- **State Used:** `ValueState<UptimeState>`
- **Watermark Assignment:** `WatermarkStrategies.getWatermarkStrategy()` -> Extracted via `txn.getReqDateTime()`.
- **Output Condition:** Operator actively authenticates for 18+ hours strictly without taking a 30-minute break.

### 34. FeatureAuthCrossBorderVelocity
- **State Used:** `ValueState<GeoVelocityState>`
- **Watermark Assignment:** `WatermarkStrategies.getWatermarkStrategy()` -> Extracted via `txn.getReqDateTime()`.
- **Output Condition:** Operator authenticates from two distinctly different Geographical State Codes in under 2 hours.

### 35. FeatureAuthConcurrentAuaBurst
- **State Used:** `ValueState<ConcurrentAuaState>`
- **Watermark Assignment:** `WatermarkStrategies.getWatermarkStrategy()` -> Extracted via `txn.getReqDateTime()`.
- **Output Condition:** Operator transactions specifically targeting 3 or more distinct AUAs (Agencies) occur identically within the exact same millisecond.

### 36. FeatureAuthBiometricReplay
- **State Used:** `ValueState<ReplayState>`
- **Watermark Assignment:** `WatermarkStrategies.getWatermarkStrategy()` -> Extracted via `txn.getReqDateTime()`.
- **Output Condition:** Operator submits the exact identical biometric envelope hash (fmr/fir/scores/pid) consecutively 3+ times.

### 37. FeatureAuthHighRiskTimeWindow
- **State Used:** `ValueState<HighRiskState>`
- **Watermark Assignment:** `WatermarkStrategies.getWatermarkStrategy()` -> Extracted via `txn.getReqDateTime()`.
- **Output Condition:** Operator registers 5 or more distinct authentication failures strictly between the hours of 1 AM and 4 AM IST.

### 38. FeatureAuthFailureRecoverySpeed
- **State Used:** `ValueState<RecoveryState>`
- **Watermark Assignment:** `WatermarkStrategies.getWatermarkStrategy()` -> Extracted via `txn.getReqDateTime()`.
- **Output Condition:** Operator registers 5+ back-to-back failures, and subsequently logs a successful authentication under 1000 milliseconds from the final failure.

### 39. FeatureAuthRapidDeviceSwitch
- **State Used:** `ValueState<RapidSwitchState>`
- **Watermark Assignment:** `WatermarkStrategies.getWatermarkStrategy()` -> Extracted via `txn.getReqDateTime()`.
- **Output Condition:** Operator physically cycles across 4 or more distinct hardware `deviceCode` identities inside a 10-minute sliding window.

### 40. FeatureAuthMultiErrorSpray
- **State Used:** `ValueState<SprayState>`
- **Watermark Assignment:** `WatermarkStrategies.getWatermarkStrategy()` -> Extracted via `txn.getReqDateTime()`.
- **Output Condition:** Operator organically triggers exactly 8 or more entirely distinct `subErrorCode` subsets within a 1-hour tumbling window.

### 41. FeatureAuthNightSurge
- **State Used:** `ValueState<SurgeState>`
- **Watermark Assignment:** `WatermarkStrategies.getWatermarkStrategy()` -> Extracted via `txn.getReqDateTime()`.
- **Output Condition:** More than 40% of the operator's rolling recent subset of 200+ requests occurs solely between the hours of 11 PM and 5 AM.

### 42. FeatureAuthConsecutiveIdenticalEvents
- **State Used:** `ValueState<IdenticalState>`
- **Watermark Assignment:** `WatermarkStrategies.getWatermarkStrategy()` -> Extracted via `txn.getReqDateTime()`.
- **Output Condition:** A literal unmodified exact payload clone across all signature properties recurs successively 3 or more times.

---

## 💡 Ideas for Future New Features!

While the Operator 360 profile is currently incredibly robust, the Flink pipeline scaling allows for unlimited real-time analytical power. Here are 5 new ideas that can be implemented next:


**2. Cross-Border Velocity Travel Anomaly**
- **Goal**: Measure impossible geographical movement speed.
- **Use Case**: An operator logs a transaction with `locationStateCode = "MH"` (Maharashtra) and 15 minutes later logs one with `locationStateCode = "DL"` (Delhi).
- **Result**: Track the previous state and timestamp, alerting if the time diff is too small to logically permit the distance between the two regions.
- **Impact**: Provides highly confident proof of VPN usage or stolen credentials.

**3. Biometric Payload Hashing & Replay Detection**
- **Goal**: Prevent the exact same cloned biometric data from being reused.
- **Use Case**: An attacker captured the data traffic of a successful fingerprint and is submitting that exact identical XML blob again.
- **Result**: Maintain a Bloom Filter or hashed `ValueState` of the `firCount` and `fmrCount` concatenations. Alert if an exact byte-for-byte match is passed multiple times across separate transactions.
- **Impact**: Strictly enforces liveliness and stops basic replay attacks.

**4. Age-to-Modality Conflict**
- **Goal**: Match human logic to the data provided.
- **Use Case**: Assuming Age data can be cross-profiled, finding an operator exclusively running "Iris" scans on users under 5 years old (which traditionally use facial exceptions). 
- **Result**: Compares demographic logic with the utilized `authType`.
- **Impact**: Finds operators exploiting non-standard authentication pathways on vulnerable demographic bands.

**5. Concurrent AUA Burst Injection**
- **Goal**: Detect advanced thread-farming.
- **Use Case**: A server-side emulator processing multiple requests for multiple completely disjointed Authentication User Agencies in the exact same millisecond. 
- **Result**: Alerts if two separate requests for two separate `aua` keys hold the exact same `eventTimestamp` down to the millisecond.
- **Impact**: Identifies server-hosted black market API hubs that act as proxies for multiple buyers at once.
