package org.simpmusic.listentogether

/**
 * iOS: the iOS `NetworkMonitor` is a no-op (NWPathMonitor is unavailable in the
 * `platform.darwin` slice this module links). The argument is accepted (and ignored)
 * to satisfy the `expect` constructor signature.
 */
actual class NetworkContext
