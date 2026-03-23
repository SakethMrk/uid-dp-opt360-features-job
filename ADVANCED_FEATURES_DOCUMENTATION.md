# Advanced Authentication Features — Comprehensive Documentation

> **Package**: `com.foo.bar.functions.advanced`
> **Version**: V3 — Behavioral Baseline & Cross-Dimensional Analysis
> **Date**: March 2026

---

## Executive Summary

These 7 features represent a fundamentally different approach to fraud detection compared to the existing feature set. While the existing features are primarily **threshold-based** (e.g., "more than 15 events/minute" or "more than 5 failures in dead of night"), these advanced features build **personalized statistical baselines** per identity and detect **deviations from that baseline**.

**Why this matters**: A threshold approach treats all identities the same. An operator who normally authenticates 20 times/hour will constantly trigger a "velocity spike" detector set at 15/min. These advanced features learn that 20/hour is NORMAL for this specific operator and only flag when behavior deviates from THEIR personal pattern.

### Feature Summary Table

| # | Feature | Detection Method | Business Signal |
|---|---------|-----------------|-----------------|
| 1 | Dormancy Resurrection | Gap analysis | Sleeper credential activation |
| 2 | Credential Velocity Profile | Welford's algorithm (z-score) | Bot automation / credential sharing |
| 3 | AUA-Device Correlation | Cross-dimensional matrix | Credential theft across agencies |
| 4 | Progressive Error Escalation | Error diversity in failure streaks | Systematic endpoint probing |
| 5 | Geo Location Anchor | Frequency-based home location | Identity used from foreign location |
| 6 | Success Ratio Cliff | Dual-window rate comparison | Transition from healthy to compromised |
| 7 | Session Identity Fingerprint | 4D behavioral DNA matching | Complete operator replacement |

---

## Feature 1: Auth Dormancy Resurrection

**File**: `FeatureAuthDormancyResurrection.java`
**Feature Name**: `auth_dormancy_resurrection`
**Feature Type**: Duration (hours)

### Business Rationale

The "cool-down attack" is one of the most sophisticated fraud patterns. An attacker steals biometric data or credentials, then deliberately waits 7+ days before using them. During this dormancy period:
- Monitoring systems lose interest (no anomalous activity)
- Stolen identity alerts expire
- Investigation teams move on to active cases

When the attacker finally activates the credential, it appears as a "normal" login — except the identity hadn't been used in over a week. This feature captures that exact moment.

### How It Works

```
Event Flow:
  Day 1-10: Identity authenticates normally (3-5 times/day)
  Day 11-17: SILENCE (no events at all)
  Day 18: Sudden successful authentication
           └─→ EMITTED: dormancy_hours = 168+ hours
```

### State Design

| Field | Type | Purpose |
|-------|------|---------|
| `lastActivityTimestamp` | long | Epoch ms of most recent event |
| `lastProcessedEventId` | String | Kafka deduplication |
| `totalEvents` | int | Minimum history enforcement (≥2) |

### Emission Criteria

1. Identity has ≥2 historical events
2. Gap between last activity and current event ≥ 7 days (604,800,000 ms)
3. Current event's authResult = "Y" (success)
4. Note: Failures after dormancy are NOT flagged (could be scanner reboot)

### Edge Cases

| Edge Case | Handling |
|-----------|----------|
| Out-of-order event | Skipped (does not update lastActivityTimestamp) |
| Kafka duplicate | Rejected via eventId comparison |
| Null authResult | Skipped |
| Negative timestamp | Falls back to `System.currentTimeMillis()` |
| First-ever event | Records timestamp, no evaluation |

### Comments Schema

```json
{
  "dormancy_hours": 172.5,
  "dormancy_days": 7.19,
  "last_seen_timestamp": "2026-03-10T14:30:00",
  "resurrection_timestamp": "2026-03-17T18:00:00",
  "total_historical_events": 45,
  "threshold_days": 7
}
```

---

## Feature 2: Auth Credential Velocity Profile

**File**: `FeatureAuthCredentialVelocityProfile.java`
**Feature Name**: `auth_credential_velocity_anomaly`
**Feature Type**: Z-Score

### Business Rationale

Every identity has a natural authentication "tempo" — a field operator might authenticate every 15-30 minutes during working hours. This tempo is as unique as a fingerprint. This feature learns the mean and standard deviation of inter-event time gaps PER IDENTITY using **Welford's online algorithm** (numerically stable, single-pass, constant memory).

After sufficient history (15+ gap samples), it flags any event whose gap deviates by more than 3 standard deviations from the personal mean.

**Key difference from `FeatureAuthVelocitySpike`**: VelocitySpike counts events per 60 seconds using a universal threshold (15). This feature builds a PERSONAL Gaussian model and flags relative deviations — meaning an operator who normally does 20 auth/hour won't trigger false positives.

### Welford's Online Algorithm

```
For each new inter-event gap x:
  count += 1
  delta  = x - mean
  mean  += delta / count
  delta2 = x - mean     (note: use updated mean)
  M2    += delta * delta2

  variance = M2 / (count - 1)    // Bessel's correction
  stddev   = sqrt(variance)
  z_score  = |x - mean| / stddev
```

This algorithm is:
- **Single-pass**: No need to store all historical gaps
- **Numerically stable**: Unlike naive variance (sum of squares), Welford's avoids catastrophic cancellation
- **Constant memory**: Only 3 doubles (count, mean, M2)

### State Design

| Field | Type | Purpose |
|-------|------|---------|
| `count` | long | Number of gap samples processed |
| `mean` | double | Running mean of gap durations (ms) |
| `m2` | double | Running sum of squared deviations |
| `lastEventTimestamp` | long | Previous event's timestamp |
| `lastEmissionTimestamp` | long | Cooldown enforcement |
| `lastProcessedEventId` | String | Kafka deduplication |

### Emission Criteria

1. At least 15 gap samples processed (MIN_SAMPLES)
2. Standard deviation > 0 (non-zero variance)
3. |current_gap - mean| / stddev ≥ 3.0 (Z_THRESHOLD)
4. At least 60 seconds since last emission (COOLDOWN)

### Direction Classification

| Direction | Meaning | Typical Cause |
|-----------|---------|---------------|
| `FASTER` | Gap is anomalously short | Bot automation, rapid-fire attack |
| `SLOWER` | Gap is anomalously long | Credential sharing, dormancy |

### Comments Schema

```json
{
  "z_score": 4.52,
  "direction": "FASTER",
  "current_gap_sec": 0.5,
  "mean_gap_sec": 1200.0,
  "stddev_gap_sec": 300.0,
  "sample_count": 45,
  "explanation": "Auth tempo is anomalously FAST compared to historical pattern — possible bot/automation"
}
```

---

## Feature 3: Auth AUA-Device Correlation

**File**: `FeatureAuthAuaDeviceCorrelation.java`
**Feature Name**: `auth_aua_device_correlation_break`
**Feature Type**: Categorical

### Business Rationale

In legitimate operations, an operator works at a FIXED agency (AUA) using a FIXED device. The `(AUA → DeviceCode)` binding is extremely stable over time. This feature builds a **cross-dimensional correlation matrix** per identity:

```
Identity "OPT123":
  AUA "A001" → Devices: {DEV_5501, DEV_5502}
  AUA "A002" → Devices: {DEV_8801}
  Known Pairs: {A001|DEV_5501, A001|DEV_5502, A002|DEV_8801}
```

When a BRAND NEW (AUA, Device) pair appears, the feature classifies the anomaly:

| Scenario | AUA Known? | Device Known? | Classification | Severity |
|----------|-----------|---------------|----------------|----------|
| A | ✅ | ❌ | `AUA_DEVICE_SHIFT` | Medium |
| B | ❌ | ✅ | `DEVICE_AUA_SHIFT` | High |
| C | ❌ | ❌ | `FULL_CORRELATION_BREAK` | Critical |
| D | ✅ | ✅ | `NEW_PAIR_EXISTING_ENTITIES` | Medium |

**Key difference from existing features**: `FeatureAuthDeviceDiversity` counts distinct devices. `FeatureAuthAuaSaDiversity` counts distinct AUAs. Neither captures the RELATIONSHIP between them. This feature does.

### Memory Safety

- AUA map capped at 50 entries (LRU eviction)
- Device map capped at 50 entries (LRU eviction)
- Pair set capped at 2500 entries (50×50 worst case)

---

## Feature 4: Auth Progressive Error Escalation

**File**: `FeatureAuthProgressiveErrorEscalation.java`
**Feature Name**: `auth_progressive_error_escalation`
**Feature Type**: Count

### Business Rationale

When a legitimate user fails authentication, they fail for the SAME reason repeatedly (dirty sensor → error 300, 300, 300). An attacker systematically probing the system produces a DIVERSIFYING pattern of distinct error codes as they try different attack vectors:

```
Legitimate user:     300, 300, 300, 300 → 1 distinct error (normal)
Systematic attacker: 300, 330, 510, 720 → 4 distinct errors (PROBING!)
```

This feature uses **composite error codes** (`errorCode:subErrorCode`) for maximum granularity — two different sub-errors under the same parent code count as different errors.

### Session Management

- **Session timeout**: 30 minutes. If failures span > 30 min, old failures are pruned (prevents cross-shift contamination)
- **Success reset**: Any success ("Y") immediately resets the streak
- **One alert per session**: After emitting, clears the streak to prevent storm

### Comments Schema

```json
{
  "error_code_sequence": ["300:01", "330:02", "510:05", "720:01"],
  "distinct_error_count": 4,
  "distinct_errors": ["300:01", "330:02", "510:05", "720:01"],
  "total_failures_in_streak": 8,
  "streak_duration_sec": 450,
  "first_failure_time": "2026-03-20T10:00:00",
  "last_failure_time": "2026-03-20T10:07:30",
  "explanation": "Identity produced 4 distinct error codes in 8 consecutive failures — indicative of systematic endpoint probing"
}
```

---

## Feature 5: Auth Geo Location Anchor

**File**: `FeatureAuthGeoLocationAnchor.java`
**Feature Name**: `auth_geo_location_anchor_drift`
**Feature Type**: Ratio

### Business Rationale

This feature builds a personalized "home location" for each identity and detects authentication from non-home locations.

**Critical difference from existing geo features**:

| Feature | What It Compares | Limitation |
|---------|-----------------|------------|
| `auth_geo_drift` | Consecutive events (A→B) | Misses slow drift. Misses repeated anomaly. |
| `auth_cross_border_velocity` | Travel speed between states | Only catches physically impossible travel |
| **This Feature** | Current vs STATISTICAL BASELINE | Catches ANY non-anchor location, regardless of when it started |

### Anchor Strength Classification

| Range | Strength | Meaning |
|-------|----------|---------|
| ≥80% | `VERY_STRONG` | Identity almost always auths from one place |
| ≥50% | `STRONG` | Clear primary location |
| <50% | `WEAK_OR_NO_ANCHOR` | Distributed usage pattern (itself anomalous) |

### Cooldown

Per-location cooldown of 1 hour prevents alert storms when identity operates from a secondary location for an extended period.

---

## Feature 6: Auth Success Ratio Cliff

**File**: `FeatureAuthSuccessRatioCliff.java`
**Feature Name**: `auth_success_ratio_cliff`
**Feature Type**: Percentage Points

### Business Rationale

This feature detects the **transition from healthy to compromised** by comparing two rolling windows:

```
Historical (50 events): 90% success rate  ← established baseline
Recent (10 events):     30% success rate  ← current behavior
Cliff:                  60 percentage points  ← ALERT!
```

**Why this is different from `FeatureAuthFailureRate`**: FailureRate uses ABSOLUTE thresholds (30%, 50%, 70%). This means:
- A user who normally has 90% success: FailureRate only triggers at 30%+ failure (70% failure rate)
- **This feature** triggers the moment their rate drops 40 points below baseline (to 50%)

**And conversely**: A user who normally has 40% success rate won't be falsely flagged when their rate dips to 30% — that's only a 10-point drop, well below the 40-point threshold.

### Recovery Mechanism

After alerting, the feature enters a "cliff alerted" state and re-enables only when the recent rate recovers to within 20% points of historical rate. This prevents alert storms during sustained degradation.

---

## Feature 7: Auth Session Identity Fingerprint

**File**: `FeatureAuthSessionIdentityFingerprint.java`
**Feature Name**: `auth_session_identity_fingerprint_break`
**Feature Type**: Similarity Score

### Business Rationale

Creates a 4-dimensional "behavioral DNA" for each identity:

| Dimension | What It Captures |
|-----------|-----------------|
| `authType` | How they authenticate (FMR, FIR, OTP, Face) |
| `modelId` | What scanner model they use |
| `deviceProviderId` | Who manufactured their device |
| `rdSoftwareId` | What software runs on the device |

Individual changes are normal:
- Software update → `rdSoftwareId` changes (1 dimension, score 3/4)
- New scanner → `modelId` changes (1 dimension, score 3/4)

**Multiple simultaneous changes are NOT normal**:
- New authType + new model + new provider = score 1/4 → **CRITICAL**: Complete identity transplant

### Similarity Score

| Score | Meaning | Severity |
|-------|---------|----------|
| 4/4 | Perfect match | Normal |
| 3/4 | One change | Not flagged (likely legitimate) |
| 2/4 | Two changes | MEDIUM — Flagged |
| 1/4 | Three changes | HIGH — Flagged |
| 0/4 | Complete transplant | CRITICAL — Flagged |

### Dominant Fingerprint Derivation

The baseline is NOT the "last seen" values — it's the MOST FREQUENT value for each dimension over the identity's entire history. This makes it robust against single-event noise.

---

## Fault-Tolerance Matrix (All Features)

| Protection | Implementation |
|-----------|----------------|
| **Kafka Duplicate Rejection** | `eventId` comparison against `lastProcessedEventId` |
| **Null Field Safety** | Every field access guarded (null/empty → skip or default) |
| **Out-of-Order Events** | Rejected for temporal features, accepted for frequency-based |
| **State TTL** | 48-hour TTL via `stateTtlFunction()` on all state descriptors |
| **Memory Bounding** | All maps/sets have configurable max size with LRU eviction |
| **Timestamp Resolution** | 3-tier fallback: `ctx.timestamp()` → `kafkaSourceTimestamp` → `currentTimeMillis()` |
| **Alert Storm Prevention** | Per-feature cooldown timers (60s to 60min depending on feature) |
| **Division-by-Zero** | Guarded in all mathematical operations |
| **Checkpointing** | All state is `ValueState` backed by RocksDB → survives pod restarts |
