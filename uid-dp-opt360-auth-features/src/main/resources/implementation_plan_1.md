# New Operator 360 Features + FeatureAuthFailureRate Bug Fix

This plan covers: (1) fixing the duplicate-timestamp bug in [FeatureAuthFailureRate](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthFailureRate.java#16-108), (2) adding 10 new fault-tolerant features with comprehensive source-table comments, and (3) creating a standalone feature documentation document.

## User Review Required

> [!IMPORTANT]
> **Duplicate Timestamp Bug Fix**: The current [FeatureAuthFailureRate](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthFailureRate.java#16-108) has no guard against events sharing the same timestamp. When two events arrive with identical `ctx.timestamp()`, both are inserted into the rolling window, potentially corrupting the failure rate calculation and streak logic. The fix will add `eventId`-based dedup + `lastTimestamp` ordering guards to prevent double-counting.

> [!WARNING]
> **State Schema Changes**: Adding new features means 10 new `ValueStateDescriptor` entries. Existing state is unaffected — there is zero compatibility risk since these are new state keys. However, after deploying, a rollback to the previous savepoint will simply ignore the new state keys (they'll be empty), which is safe.

---

## Proposed Changes

### Bug Fix: FeatureAuthFailureRate Duplicate Timestamp Guard

#### [MODIFY] [FeatureAuthFailureRate.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthFailureRate.java)

Add a `lastProcessedEventId` field to [RateState](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthFailureRate.java#22-28). Before processing, check if the incoming event's `eventId` matches the last processed one — if so, skip it. Also add a `lastTimestamp` guard: if a new event has the same timestamp as the previous event but a different `eventId`, it's valid but we ensure ordering is deterministic (process both, but don't let timestamp collision corrupt the sliding window). This makes the feature completely idempotent against Kafka redeliveries and duplicate timestamp events.

---

### New TXN Features (10 Features)

> [!NOTE]
> **Fault-Tolerance Design Philosophy** applied to ALL new features:
> - **Event Dedup**: Every feature stores `lastProcessedEventId` in its state and skips events with duplicate `eventId`, preventing corruption from Kafka redeliveries or same-timestamp duplicates
> - **Checkpoint-Safe**: All state is serializable POJOs stored in RocksDB `ValueState` — fully recoverable from checkpoints/savepoints
> - **Timer Cleanup**: Time-windowed features register [onTimer](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthVelocitySpike.java#73-86) callbacks to clean up expired state, preventing memory leaks if an operator goes silent
> - **Null-Safe**: Every field access is null-guarded with safe parsing helpers
> - **Comprehensive Comments**: Every feature includes detailed Javadoc linking to the source table fields used (`InputMessageTxn.*` / `InputMessageBio.*`)

---

#### [NEW] [FeatureAuthGhostUptime.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthGhostUptime.java)

**Goal**: Detect non-human endurance — an operator actively authenticating for 18+ hours without a 30-minute inactivity gap.

**Source Fields**: `eventId`, `optId`, event timestamp from `reqDateTime`

**Logic**: Maintain `activityStartTimestamp`, `lastActivityTimestamp`, and `maxGapSeenMs`. On each event, check gap from last activity. If gap > 30min, reset the activity window. If [(currentTs - activityStartTimestamp) > 18 hours](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthGeoDrift.java#33-39) and no 30min gap exists, emit alert.

---

#### [NEW] [FeatureAuthCrossBorderVelocity.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthCrossBorderVelocity.java)

**Goal**: Detect impossible geo movement — operator changes state in less time than physically possible.

**Source Fields**: `locationStateCode`, `locationDistrictCode`, event timestamp

**Logic**: Maintain `lastStateCode`, `lastDistrictCode`, `lastTimestamp`. When stateCode changes, compute `timeDiff`. If different state within 30 minutes, it's suspicious. If different state within 10 minutes, it's extremely suspicious (flag). Uses a static distance category map (same-state=OK, adjacent-state=1h-minimum, far-state=3h-minimum).

---

#### [NEW] [FeatureAuthConcurrentAuaBurst.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthConcurrentAuaBurst.java)

**Goal**: Detect thread-farming — same-millisecond requests for multiple distinct AUAs.

**Source Fields**: `aua`, `eventTimestamp`, event timestamp

**Logic**: Maintain a `Map<Long, Set<String>>` mapping timestamps (ms-precision) to distinct AUA values. If any single timestamp has 2+ distinct AUAs, emit alert. Prune entries older than 60 seconds. This catches server-hosted API hubs processing requests for multiple agencies simultaneously.

---

#### [NEW] [FeatureAuthBiometricReplay.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthBiometricReplay.java)

**Goal**: Detect replay attacks — same biometric signature submitted multiple times.

**Source Fields**: `fmrCount`, `firCount`, `fingerMatchScore`, `faceMatchScore`, `pidSize`

**Logic**: Create a fingerprint hash from `fmrCount + "|" + firCount + "|" + fingerMatchScore + "|" + faceMatchScore + "|" + pidSize`. Maintain a bounded `LinkedHashSet<String>` (max 200 entries, LRU eviction). If the hash already exists in the set, increment a `replayHitCount`. Alert when `replayHitCount >= 3` within the window.

---

#### [NEW] [FeatureAuthSessionGapPattern.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthSessionGapPattern.java)

**Goal**: Detect scripted automation via regular inter-event timing patterns.

**Source Fields**: event timestamp (derived from `reqDateTime`)

**Logic**: Maintain a rolling buffer of the last 20 inter-event gaps (ms). Compute coefficient of variation (stddev/mean). A CV < 0.15 means the gaps are suspiciously uniform (like a script doing `Thread.sleep(5000)`). Human operators have highly variable timing.

---

#### [NEW] [FeatureAuthFailureRecoverySpeed.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthFailureRecoverySpeed.java)

**Goal**: Detect bot-like instant recovery from failure streaks.

**Source Fields**: `authResult`, event timestamp

**Logic**: Track consecutive failures. When a success event arrives after 5+ consecutive failures, compute `recoveryGap = successTs - lastFailureTs`. If `recoveryGap < 500ms`, it's suspiciously fast (a bot retrying at machine speed). Humans need at least a few seconds to re-scan biometrics.

---

#### [NEW] [FeatureAuthSuccessRateShift.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthSuccessRateShift.java)

**Goal**: Detect sudden changes in success rate using CUSUM (Cumulative Sum) change-point detection.

**Source Fields**: `authResult`, event timestamp

**Logic**: After 50 events baseline, compute running success rate. Use CUSUM algorithm: maintain `S_pos` and `S_neg` accumulators. When either exceeds threshold `h=5.0`, a statistically significant shift has occurred. This catches operators who suddenly start succeeding (compromised system) or failing (device breakdown) at abnormal rates.

---

#### [NEW] [FeatureAuthMultiErrorSpray.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthMultiErrorSpray.java)

**Goal**: Detect operators probing the system by cycling through many distinct error codes.

**Source Fields**: `errorCode`, `subErrorCode`, event timestamp

**Logic**: Maintain a `Set<String>` of distinct error codes seen in a 1-hour tumbling window. If an operator triggers 8+ distinct error codes in one hour, they're likely probing for vulnerabilities. Track count of total errors alongside unique codes.

---

#### [NEW] [FeatureAuthNightSurge.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthNightSurge.java)

**Goal**: Detect late-night activity surges (11 PM – 5 AM IST) beyond what's normal.

**Source Fields**: event timestamp (derived from `reqDateTime`)

**Logic**: Maintain `nightCount` and `dayCount` over a rolling 200-event window. If `nightRatio >= 0.40` (40%+ of transactions happen at night), emit alert. Uses IST timezone conversion. Separate from `FeatureAuthOddHourRatio` which only tracks odd hours; this specifically targets the most suspicious midnight-to-dawn window with a higher sensitivity threshold.

---

#### [NEW] [FeatureAuthConsecutiveIdenticalEvents.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthConsecutiveIdenticalEvents.java)

**Goal**: Detect exact-duplicate consecutive events (replay attack at the network level).

**Source Fields**: `authCode`, `aua`, [sa](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/OutMessage.java#10-63), `authType`, `authResult`, `errorCode`, `deviceCode`, `pidSize`, `fmrCount`

**Logic**: Create a composite signature from multiple fields: `authType + "|" + aua + "|" + sa + "|" + errorCode + "|" + deviceCode + "|" + pidSize + "|" + fmrCount`. If 3+ consecutive events produce the same signature, emit alert. This catches raw network-level packet replays that duplicate entire request bodies.

---

### Integration Files

#### [MODIFY] [StateDescriptors.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/StateDescriptors.java)

Add 10 new `ValueStateDescriptor` entries for all new features.

#### [MODIFY] [FlinkPipeline.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/pipeline/FlinkPipeline.java)

Wire 10 new `DataStream<OutMessage>` streams into the pipeline union.

---

### Documentation

#### [NEW] [FEATURES_V2.md](file:///d:/Codebase/uid-dp-opt360-features-job/FEATURES_V2.md)

Comprehensive standalone document detailing all 10 new features with:
- Feature name, ID, version
- Source table fields used (with column-level traceability)
- Algorithm description
- Threshold values and alert conditions
- Fault-tolerance guarantees
- JSON comments schema for each feature
- Edge cases handled

#### [MODIFY] [README.md](file:///d:/Codebase/uid-dp-opt360-features-job/README.md)

Add summaries of all 10 new features to the existing feature breakdown, numbered 33–42.

---

## Verification Plan

### Compilation Check
- **Command**: `mvn compile -f d:\Codebase\uid-dp-opt360-features-job\uid-dp-opt360-auth-features\pom.xml`
- Validates that all new files compile, all state descriptors are properly typed, and the pipeline successfully wires all new streams.

### Manual Verification
- Review the generated files to ensure:
  1. Every new feature has `lastProcessedEventId` dedup in its state class
  2. Every feature has comprehensive Javadoc comments with source table field references
  3. All [StateDescriptors](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/StateDescriptors.java#10-154) entries match their respective feature state classes
  4. All pipeline streams are in the union
  5. The [FeatureAuthFailureRate](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureAuthFailureRate.java#16-108) bug fix properly handles duplicate timestamps
