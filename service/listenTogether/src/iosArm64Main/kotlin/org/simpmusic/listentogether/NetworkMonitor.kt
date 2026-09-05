package org.simpmusic.listentogether

/**
 * iOS no-op. NWPathMonitor would be the right primitive but is not available in the
 * `platform.darwin` slice this module links; a no-op keeps the surface uniform and the
 * `forceReconnect()` path is still reachable from the explicit user actions (Pause / Play / etc.).
 *
 * The [context] parameter is accepted (and ignored) to satisfy the [NetworkMonitor] expect
 * constructor signature on iOS.
 */
actual class NetworkMonitor actual constructor(context: NetworkContext) {
    actual fun start(onTransportChanged: () -> Unit) = Unit
    actual fun stop() = Unit
    actual fun currentTransport(): String? = null
}
