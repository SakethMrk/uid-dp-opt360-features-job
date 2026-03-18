# Opt360 Biological Spoofing Defenses (V1)

*Target: `InputMessageBio` | Scope: Analog & Algorithmic Subversion*

While traditional ML anomaly detection identifies generic drift, advanced attackers actively bypass the physical camera sensor entirely or override the Android/iOS client math. The features in this package are designed to detect **physical and programmatic impossibilities**.

## 1. FeatureBioCaptureTimeSpoof
- **Logic**: Tracks the precise delta between `livenessRequestInitiatedTime` and `livenessResponseReceivedTime`.
- **The Impossibility**: Hardware cameras physically require >150ms to open the shutter, expose the sensor to light, render the image bytes, and pass it to the OS. 
- **The Attack**: If the delta is magically `< 50ms`, the app's camera hook has been hijacked, and a pre-recorded `.mp4` video is being injected directly into the socket.

## 2. FeatureBioLivenessDesync
- **Logic**: Compares `biolivenessEndtime` with the primary `kafkaSourceTimestamp`.
- **The Impossibility**: A live transaction happens right now. 
- **The Attack**: A "Token Replay" attack buffers a mathematically perfect, valid liveness check from weeks ago, saving it to disk, and attaching it to a newly forged transaction today. If the liveness token is >60 minutes older than the transaction, it's a replay.

## 3. FeatureBioExactImageSizeReplay
- **Logic**: Alerts if an operator submits 4 consecutive distinct transactions that possess the *exact identical byte count* in `imageSize`.
- **The Impossibility**: Real lighting conditions change microscopically every millisecond. No two consecutive JPEGs/PNGs of a human face will ever compress to the exact identical byte size down to the single digit.
- **The Attack**: The operator is uploading the exact same static `.jpg` file repeatedly.

## 4. FeatureBioUncannyValleyScorePattern
- **Logic**: Tracks `faceLivenessScore` over 5 consecutive transactions for exactly identical value clusters.
- **The Impossibility**: Human biology naturally wavers. Generating 5 distinct authentications that hit *exactly* `0.9845` without variance is impossible.
- **The Attack**: An AI Deepfake generator is looping its output, targeting the exact minimum threshold necessary to pass the API, resulting in perfectly uniform, un-varying scores.

## 5. FeatureBioDeepPrintSubversion
- **Logic**: Triggers heavily when the boolean `deepPrintIsMatched` is asserted as `"true"` or `"Y"`, but the algorithmic `deepPrintFIRMatchScore` is <= `5.0` (or `null`/`0`).
- **The Impossibility**: The math failed, but the boolean passed.
- **The Attack**: The client installed a tampered APK that intercepts the `return false;` output from the biometric library and flips it to `true`, while successfully sending the underlying low raw scores.

---

### Fault Tolerance Matrix
*   **Idempotency**: All `ValueState` objects inherently track and drop duplicated `eventId` payloads.
*   **Safety**: Explicit `NumberFormatException` catching around all string-to-long conversions (`Long.parseLong()`).
*   **GC Collection**: Exclusively `ValueState` usage synced with 24-hour `StateTtlConfig` prevents KeyedProcess unbounded memory leaks on stale operators.
