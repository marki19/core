package com.maxrave.data.di.loader

import com.maxrave.data.di.databaseModule
import com.maxrave.data.di.listenTogetherModule
import com.maxrave.data.di.mediaHandlerModule
import com.maxrave.data.di.platformListenTogetherModule
import com.maxrave.data.di.repositoryModule
import org.koin.core.context.loadKoinModules

fun loadAllModules() {
    loadKoinModules(
        listOf(
            databaseModule,
            repositoryModule,
        ),
    )
    loadKoinModules(mediaHandlerModule)
    // NOTE: `createdAtStart` is NOT enough for a module loaded this way — nothing constructs it
    // unless something injects it, and the bridge exists purely to listen, so nobody would.
    // ListenTogetherViewModel injects and starts it; start() is idempotent.
    loadKoinModules(listenTogetherModule)
    // Platform-specific additions: on Android this provides a real PARTIAL_WAKE_LOCK, on
    // Desktop and iOS a no-op. It must be loaded AFTER listenTogetherModule so Koin's
    // resolution order is the same on every platform.
    loadKoinModules(platformListenTogetherModule)
    loadMediaService()
}

expect fun loadMediaService()