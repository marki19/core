package org.simpmusic.listentogether

import android.content.Context
import android.os.PowerManager

/**
 * Android implementation of [JamWakeLockManager] using `PowerManager.newWakeLock(PARTIAL_WAKE_LOCK)`.
 *
 * The wake lock keeps the CPU running while the screen may sleep. This implementation respects
 * the base class's reference-counted contract: each [acquire] call increments Android's internal
 * counter, and the OS lock is released only when the count reaches zero.
 *
 * Obtain from Koin via `com.maxrave.data.di.platformListenTogetherModule` in `core-data`: it
 * registers this as `single<JamWakeLockManager>`, replacing the no-op base class on Android.
 */
class AndroidJamWakeLockManager(
    private val context: Context,
) : JamWakeLockManager() {

    /** Re-acquire well before this so a long room never goes unprotected. */
    private val wakeLockTimeoutMs = 6 * 60 * 60 * 1000L // 6 hours

    private val wakeLock: PowerManager.WakeLock by lazy {
        val powerManager = context.getSystemService(Context.POWER_SERVICE) as PowerManager
        powerManager.newWakeLock(
            PowerManager.PARTIAL_WAKE_LOCK,
            "SimpMusic:JamSession",
        )
    }

    override fun acquire() {
        // Do NOT check isHeld here. We MUST call acquire() every time to let Android's
        // reference counter increment properly. The base class contract is:
        // each acquire() must be balanced by a release() from the same caller.
        wakeLock.acquire(wakeLockTimeoutMs)
    }

    override fun release() {
        // Check isHeld to prevent "Under-locked" RuntimeException if release() is called
        // more times than acquire() by mistake.
        if (wakeLock.isHeld) {
            try {
                wakeLock.release()
            } catch (_: RuntimeException) {
                // Failsafe for aggressive OS garbage collection.
            }
        }
    }

    override fun isHeld(): Boolean = wakeLock.isHeld
}
