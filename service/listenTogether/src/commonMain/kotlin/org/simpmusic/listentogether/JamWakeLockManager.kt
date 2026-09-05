package org.simpmusic.listentogether

/**
 * Manages a WakeLock to prevent CPU sleep during Listen Together sessions.
 *
 * The CPU must stay awake for the ping loop to run; if it sleeps, the socket times out and
 * reconnection begins. On Android we use `PARTIAL_WAKE_LOCK` (CPU on, screen allowed to sleep).
 * On Desktop and iOS this is a no-op — the JVM and Darwin keep the thread alive without one.
 *
 * This is a **base class** with no-op implementations. Platform-specific implementations
 * extend it: `AndroidJamWakeLockManager` on Android, and the no-op base class is used directly
 * on Desktop and iOS.
 *
 * Obtain from Koin: the platform's `platformListenTogetherModule` registers the right
 * implementation as a `single<JamWakeLockManager>`. If no registration exists, callers fall
 * back to the no-op singleton [noOp].
 *
 * Reference-counted: [acquire] is balanced by [release], and the underlying OS lock is acquired
 * once on first acquire and released only when the count reaches zero.
 */
open class JamWakeLockManager {
    /** Acquire the wake lock. Override in subclasses for platform behavior. */
    open fun acquire() {}

    /**
     * Release the wake lock. If [acquire] has been called more times than [release], the
     * underlying lock remains held.
     */
    open fun release() {}

    /** True if the wake lock is currently held. */
    open fun isHeld(): Boolean = false

    companion object {
        /**
         * A shared no-op instance. Used as the Koin default when no platform-specific registration
         * exists, and as the direct implementation on Desktop and iOS.
         */
        val noOp: JamWakeLockManager by lazy { JamWakeLockManager() }
    }
}
