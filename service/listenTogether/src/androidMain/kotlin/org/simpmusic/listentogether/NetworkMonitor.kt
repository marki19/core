package org.simpmusic.listentogether

import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest

/**
 * Android implementation backed by [ConnectivityManager.NetworkCallback].
 *
 * The transport class is derived from the `NET_CAPABILITY_*` bits:
 *  - `TRANSPORT_WIFI`, `TRANSPORT_ETHERNET`, `TRANSPORT_VPN` → `"wifi"`
 *  - `TRANSPORT_CELLULAR` → `"cellular"`
 *  - anything else → `"other"`
 *
 * Only the transport CLASS triggers a handoff, so moving between two Wi-Fi access points is silent.
 */
actual class NetworkMonitor actual constructor(
    private val context: NetworkContext,
) {
    private var callback: ConnectivityManager.NetworkCallback? = null
    private var lastTransport: String? = null
    private var onChanged: (() -> Unit)? = null

    actual fun start(onTransportChanged: () -> Unit) {
        if (callback != null) return
        onChanged = onTransportChanged
        lastTransport = currentTransport()

        val ctx = context.context
        val cm = ctx.getSystemService(android.content.Context.CONNECTIVITY_SERVICE) as ConnectivityManager
        val request =
            NetworkRequest.Builder()
                .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                .build()

        val cb =
            object : ConnectivityManager.NetworkCallback() {
                override fun onAvailable(network: Network) {
                    val caps = cm.getNetworkCapabilities(network) ?: return
                    emitIfChanged(transportOf(caps))
                }

                override fun onCapabilitiesChanged(
                    network: Network,
                    caps: NetworkCapabilities,
                ) {
                    emitIfChanged(transportOf(caps))
                }

                override fun onLost(network: Network) {
                    // Lost networks do not change the transport class — the next available network
                    // (or `onUnavailable`) will.
                }
            }

        try {
            cm.registerNetworkCallback(request, cb)
            callback = cb
        } catch (_: SecurityException) {
            // ACCESS_NETWORK_STATE not granted (declared in the library AndroidManifest.xml; the
            // app manifest also declares it, so this branch is unreachable in practice).
            callback = null
        }
    }

    actual fun stop() {
        val cb = callback ?: return
        try {
            val ctx = context.context
            val cm = ctx.getSystemService(android.content.Context.CONNECTIVITY_SERVICE) as ConnectivityManager
            cm.unregisterNetworkCallback(cb)
        } catch (_: IllegalArgumentException) {
            // Already unregistered; harmless.
        }
        callback = null
        onChanged = null
        lastTransport = null
    }

    @Suppress("unused")
    actual fun currentTransport(): String? {
        val ctx = context.context
        val cm = ctx.getSystemService(android.content.Context.CONNECTIVITY_SERVICE) as ConnectivityManager
        val active = cm.activeNetwork ?: return null
        val caps = cm.getNetworkCapabilities(active) ?: return null
        return transportOf(caps)
    }

    private fun emitIfChanged(transport: String) {
        val previous = lastTransport
        if (previous == null) {
            lastTransport = transport
            return
        }
        if (transport == previous) return
        lastTransport = transport
        onChanged?.invoke()
    }

    private fun transportOf(caps: NetworkCapabilities): String =
        when {
            caps.hasTransport(NetworkCapabilities.TRANSPORT_WIFI) -> "wifi"
            caps.hasTransport(NetworkCapabilities.TRANSPORT_ETHERNET) -> "wifi"
            caps.hasTransport(NetworkCapabilities.TRANSPORT_VPN) -> "wifi"
            caps.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR) -> "cellular"
            else -> "other"
        }
}
