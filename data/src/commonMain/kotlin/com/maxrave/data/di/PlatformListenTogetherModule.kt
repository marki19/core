package com.maxrave.data.di

import org.koin.dsl.module

/**
 * Platform-specific additions to [listenTogetherModule].
 *
 * On Android, this provides a real `PARTIAL_WAKE_LOCK` for Jam sessions. On Desktop and iOS,
 * the no-op base class singleton is used. The [listenTogetherModule] in commonMain always
 * calls `get<JamWakeLockManager>()` — on Android, Koin resolves it to [AndroidJamWakeLockManager];
 * on other platforms, it falls back to [org.simpmusic.listentogether.JamWakeLockManager.noOp].
 */
expect val platformListenTogetherModule: org.koin.core.module.Module
