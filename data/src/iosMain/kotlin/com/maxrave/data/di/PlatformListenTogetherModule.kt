package com.maxrave.data.di

import org.koin.dsl.module
import org.simpmusic.listentogether.JamWakeLockManager
import org.simpmusic.listentogether.NetworkMonitor

/**
 * iOS platform module: a no-op wake lock. iOS apps use `UIApplication.shared.beginBackgroundTask`
 * for short background survival, but no analog to Android's PARTIAL_WAKE_LOCK is available from
 * a KMP module.
 *
 * `NetworkMonitor` is also a no-op on iOS (no `NWPathMonitor` in the `platform.darwin` slice).
 * The constructor argument is `Unit` because the iOS `NetworkContext` typealias resolves to
 * `Unit`; the actual is accepted and ignored.
 */
actual val platformListenTogetherModule = module {
    single<JamWakeLockManager> { JamWakeLockManager.noOp }
    single { NetworkMonitor(org.simpmusic.listentogether.NetworkContext()) }
}
