package com.maxrave.data.di

import org.koin.android.ext.koin.androidContext
import org.koin.dsl.module
import org.simpmusic.listentogether.AndroidJamWakeLockManager
import org.simpmusic.listentogether.JamWakeLockManager
import org.simpmusic.listentogether.NetworkMonitor

/**
 * Android-specific Listen Together module: registers a real `PARTIAL_WAKE_LOCK` so the WebSocket
 * can survive longer screen-off periods and pongs continue to land while the host pauses playback.
 *
 * Also registers the [NetworkMonitor] which calls `forceReconnect()` on transport-type handoffs
 * (Wi-Fi → cellular and back).
 *
 * `androidContext()` supplies the application [android.content.Context] that both
 * [AndroidJamWakeLockManager] and the Android [NetworkMonitor] need. On JVM and iOS the
 * `NetworkContext` typealias resolves to `Unit`, so the same factory shape compiles.
 */
actual val platformListenTogetherModule = module {
    single<JamWakeLockManager> { AndroidJamWakeLockManager(androidContext()) }
    single { NetworkMonitor(org.simpmusic.listentogether.NetworkContext(androidContext())) }
}