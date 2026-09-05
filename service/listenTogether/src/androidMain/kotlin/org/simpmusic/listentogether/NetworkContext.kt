package org.simpmusic.listentogether

import android.content.Context

/** Android: an application [Context] is what [NetworkMonitor] needs to access `ConnectivityManager`. */
actual class NetworkContext(val context: Context)
