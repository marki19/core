package com.maxrave.data.di

import org.koin.dsl.module
import org.simpmusic.listentogether.JamWakeLockManager
import org.simpmusic.listentogether.NetworkMonitor

/**
 * Desktop (JVM) platform module: a no-op wake lock. The JVM keeps threads alive without one.
 *
 * We still register a binding so Koin can resolve the `JamWakeLockManager` type without falling
 * back to a global default — same shape as the Android module, just inert.
 *
 * `NetworkMonitor` is the JVM actual: a daemon thread polling `NetworkInterface` for transport
 * changes (the JDK does not expose kernel-level handoff events). The constructor argument
 * is `Unit` because the JVM `NetworkContext` typealias resolves to `Unit`; the actual is
 * accepted and ignored.
 */
actual val platformListenTogetherModule = module {
    single<JamWakeLockManager> { JamWakeLockManager.noOp }
    single { NetworkMonitor(org.simpmusic.listentogether.NetworkContext()) }
}
