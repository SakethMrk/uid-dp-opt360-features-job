# Operator 360 — BIO Feature Game Plan & Pseudocode

> For you to implement on your own! Each feature uses [InputMessageBio](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/InputMessageBio.java#8-55) fields, keyed by `optId`.

---

## BIO Fields Available

```
optId, bioType, matchScore, isLive, isMatched,
livenessRequestInitiatedTime, livenessResponseReceivedTime,
faceLivenessScore, imageSize, imageFormat,
deepPrintIsMatched, deepPrintFIRMatchScore, deepPrintResponseTime,
deepPrintFingerPositon, deepPrintStartTime, deepPrintEndTime,
bodyPartTypeProbe, probeMinutiae, probeSize,
isLivenessCheckInvoked, islivenessEnabled,
bioLivenessResponseTime, deviceCode, errorCode,
faceMatchScore, faceMatchResponseTime,
isAimlFaceMatcherAllowed, isFaceTemplate,
faceLivenessClientRequestId
```

---

## Feature B1: `bio_match_score_trend_v1`

**Why:** Declining match scores over time = sensor degradation or spoofing with deteriorating fake biometrics.

```
CLASS FeatureBioMatchScoreTrend extends KeyedProcessFunction<String, InputMessageBio, OutMessage>

    STATE: {
        LinkedList<Double> recentScores  // last 10 scores
    }

    PROCESS(bio):
        score = parseDouble(bio.matchScore)
        IF score == null OR score <= 0: RETURN

        state.recentScores.addLast(score)
        IF state.recentScores.size() > 10:
            state.recentScores.removeFirst()

        IF state.recentScores.size() >= 10:
            slope = computeLinearRegressionSlope(state.recentScores)
            // slope formula: Σ((i - mean_i)(y_i - mean_y)) / Σ((i - mean_i)²)
            IF slope < -2.0:
                EMIT feature(
                    name = "bio_match_score_trend_v1",
                    value = slope,
                    comments = JSON { "recent_scores": recentScores, "slope": slope }
                )

    // Helper: Linear regression slope over index positions 0..N-1
    computeLinearRegressionSlope(scores):
        n = scores.size()
        mean_x = (n - 1) / 2.0
        mean_y = sum(scores) / n
        numerator = 0, denominator = 0
        FOR i = 0 to n-1:
            numerator   += (i - mean_x) * (scores[i] - mean_y)
            denominator += (i - mean_x)²
        RETURN numerator / denominator
```

---

## Feature B2: `bio_liveness_failure_streak_v1`

**Why:** Consecutive liveness check failures = spoofing attempts or defective sensors.

```
CLASS FeatureBioLivenessFailureStreak extends KeyedProcessFunction<String, InputMessageBio, OutMessage>

    STATE: {
        int streakCount,
        long firstFailureTs,
        long lastFailureTs,
        String bioType,
        Set<String> deviceCodes
    }

    PROCESS(bio):
        isLive = bio.getIsLive()

        IF isLive != "Y" AND isLive != null:
            // FAILURE - extend streak
            IF state.streakCount == 0:
                state.firstFailureTs = context.timestamp()
                state.bioType = bio.getBioType()
            state.streakCount++
            state.lastFailureTs = context.timestamp()
            IF bio.deviceCode != null: state.deviceCodes.add(bio.deviceCode)
            state.update()

        ELSE IF isLive == "Y":
            // SUCCESS - streak broken, emit if streak > 0
            IF state.streakCount > 0:
                EMIT feature(
                    name = "bio_liveness_failure_streak_v1",
                    value = state.streakCount,
                    comments = JSON {
                        "streak_count": state.streakCount,
                        "bio_type": state.bioType,
                        "first_failure": formatTs(state.firstFailureTs),
                        "last_failure": formatTs(state.lastFailureTs),
                        "device_codes": state.deviceCodes
                    }
                )
            state.clear()
```

> **Hint:** This is very similar to your [FeatureLivenessStreakScore](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/functions/FeatureLivenessStreakScore.java#18-112) — you already know this pattern! 💪

---

## Feature B3: `bio_response_time_anomaly_v1`

**Why:** Too-fast = bypassed check. Too-slow = MITM or network injection.

```
CLASS FeatureBioResponseTimeAnomaly extends KeyedProcessFunction<String, InputMessageBio, OutMessage>

    STATE: {
        long count,
        double mean,
        double m2      // for Welford's online variance
    }

    PROCESS(bio):
        startTime = parseLong(bio.livenessRequestInitiatedTime)
        endTime   = parseLong(bio.livenessResponseReceivedTime)
        IF startTime <= 0 OR endTime <= 0: RETURN

        responseTime = endTime - startTime  // in ms

        // Welford's online algorithm
        state.count++
        delta = responseTime - state.mean
        state.mean += delta / state.count
        delta2 = responseTime - state.mean
        state.m2 += delta * delta2

        IF state.count >= 10:
            variance = state.m2 / (state.count - 1)
            stddev = sqrt(variance)
            IF stddev > 0:
                zScore = abs(responseTime - state.mean) / stddev
                IF zScore > 3.0:
                    EMIT feature(
                        name = "bio_response_time_anomaly_v1",
                        value = zScore,
                        comments = JSON {
                            "response_time_ms": responseTime,
                            "mean_ms": state.mean,
                            "stddev_ms": stddev,
                            "event_count": state.count
                        }
                    )
        state.update()
```

---

## Feature B4: `bio_deepprint_failure_rate_v1`

**Why:** High DeepPrint failure rate = presentation attacks (fake fingers).

```
CLASS FeatureBioDeepPrintAnomaly extends KeyedProcessFunction<String, InputMessageBio, OutMessage>

    STATE: {
        LinkedList<Boolean> recentResults,  // last 20
        int failureCount
    }

    PROCESS(bio):
        isMatched = bio.getDeepPrintIsMatched()
        IF isMatched == null: RETURN

        matched = "Y".equalsIgnoreCase(isMatched)
        state.recentResults.addLast(matched)
        IF NOT matched: state.failureCount++

        IF state.recentResults.size() > 20:
            removed = state.recentResults.removeFirst()
            IF NOT removed: state.failureCount--

        IF state.recentResults.size() >= 20:
            failureRate = state.failureCount / 20.0
            IF failureRate > 0.30:
                EMIT feature(
                    name  = "bio_deepprint_failure_rate_v1",
                    value = failureRate,
                    comments = JSON { "failures": state.failureCount, "window": 20 }
                )
        state.update()
```

---

## Feature B5: `bio_face_liveness_score_decline_v1`

**Why:** `faceLivenessScore` dropping over time = photo-of-photo attack adaptation.

```
// Same pattern as B1 (match_score_trend)
// but on: bio.getFaceLivenessScore()
// Threshold: slope < -1.5
// Window: last 8 scores
```

---

## Feature B6: `bio_modality_concentration_v1`

**Why:** If an operator only ever does one `bioType` (e.g., only fingerprint, never iris/face), it could indicate device limitation or bypassed modalities.

```
STATE: { Map<String, Integer> bioTypeCounts, int total }

PROCESS(bio):
    type = bio.getBioType()
    IF type == null: RETURN
    state.bioTypeCounts[type]++
    state.total++

    IF state.total >= 30:
        maxCount = max(state.bioTypeCounts.values())
        concentration = maxCount / state.total
        IF concentration > 0.95:
            EMIT feature(
                name = "bio_modality_concentration_v1",
                value = concentration,
                comments = JSON { "dominant_type": dominantKey, "counts": bioTypeCounts }
            )
```

---

## Feature B7: `bio_image_size_anomaly_v1`

**Why:** Unusually small `imageSize` = compressed/fake images. Unusually large = injected high-res outside normal capture.

```
// Same Welford's pattern as B3
// but on: parseLong(bio.getImageSize())
// z-score threshold: 3.0
```

---

## Feature B8: `bio_probe_minutiae_anomaly_v1`

**Why:** `probeMinutiae` count should be consistent for real fingers. Very low = partial contact. High variance = multiple different fingers.

```
// Same stddev pattern as TXN E3 (fmr_count_anomaly)
// but on: parseLong(bio.getProbeMinutiae())
// Track last 10 values, emit if stddev > 15
```

---

## 🧩 What You Need to Build for BIO Features

1. **`WatermarkStrategiesBio.java`** — watermark assigner for [InputMessageBio](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/InputMessageBio.java#8-55) using `eventTimestamp`
2. **Wire in [FlinkPipeline.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/pipeline/FlinkPipeline.java)** — process `inStream2` through your BIO features
3. **State descriptors** — add to [StateDescriptors.java](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/StateDescriptors.java) for each BIO feature
4. **Union** — BIO features emit [OutMessage](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/OutMessage.java#10-63), so they can join the union stream

> The patterns are identical to what you've already built — the only change is the input type ([InputMessageBio](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/InputMessageBio.java#8-55) instead of [InputMessageTxn](file:///d:/Codebase/uid-dp-opt360-features-job/uid-dp-opt360-auth-features/src/main/java/com/foo/bar/dto/InputMessageTxn.java#8-60)).
