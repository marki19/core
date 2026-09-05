package org.simpmusic.listentogether

/**
 * Cross-platform alias for whatever the platform considers an "application context".
 *
 * Android passes `android.content.Context`; JVM and iOS ignore the argument (the JVM polls
 * `NetworkInterface` without one, and iOS's no-op `NetworkMonitor` does not start). The
 * alias is the only commonMain-visible surface; the actual platform types live in the
 * `expect` declarations of their respective source sets and are provided by Koin through
 * the platform's `platformListenTogetherModule`.
 */
expect class NetworkContext

/**
 * Watches for transport-type changes (Wi-Fi → cellular, etc.) and asks the session to reconnect
 * without waiting for the backoff to fire.
 *
 * The reconnect loop is already time-budgeted (5 minutes) and a transport handoff is a continuation
 * of the same room, NOT a new session — see [ListenTogetherSession.forceReconnect]. The network
 * monitor is therefore expected to call [onTransportChanged] from a background callback, not from
 * the UI thread.
 *
 * Platform behaviour:
 *  - **Android**: `ConnectivityManager.NetworkCallback` listening for `onCapabilitiesChanged`
 *    and `onLost`. Only the *transport class* (Wi-Fi / cellular / ethernet) matters, not the
 *    specific network — moving between two Wi-Fi access points should not force a reconnect.
 *  - **JVM**: `java.net.NetworkInterface` polling on a daemon thread. No kernel-level handoff
 *    events exist on the JDK, so a 1-second poll is the closest equivalent.
 *  - **iOS**: `NWPathMonitor` over `nwPathMonitorSetUpdateHandler`.
 *
 * The monitor holds a weak reference to the session so it can never pin the process in memory:
 * dropping the session is what stops the watch.
 */
expect class NetworkMonitor(context: NetworkContext) {
    /**
     * Start watching for transport changes. The supplied [onTransportChanged] is invoked from
     * a background thread whenever the active transport class differs from the previous one.
     */
    fun start(onTransportChanged: () -> Unit)

    /** Stop watching. Safe to call multiple times. */
    fun stop()

    /** Best-effort read of the current transport class — `null` if it cannot be determined. */
    fun currentTransport(): String?
}
