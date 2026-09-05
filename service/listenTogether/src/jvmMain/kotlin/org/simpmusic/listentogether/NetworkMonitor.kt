package org.simpmusic.listentogether

import java.net.NetworkInterface
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * JVM implementation backed by polling [NetworkInterface].
 *
 * The JDK does not expose a kernel-level handoff event; we sample the active interface name every
 * second and treat a change as a transport handoff. The interface name is the best signal the JDK
 * offers — `en0` → `pdp_ip0` is the macOS / Linux equivalent of "Wi-Fi → cellular".
 *
 * The [context] parameter is accepted (and ignored) to satisfy the [NetworkMonitor] expect
 * constructor signature on JVM: this implementation has no dependency on an Android-style Context.
 */
actual class NetworkMonitor actual constructor(context: NetworkContext) {

    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "ListenTogether-NetworkMonitor").apply { isDaemon = true }
    }
    private var onChanged: (() -> Unit)? = null
    private var lastInterface: String? = null
    @Volatile private var running = false

    actual fun start(onTransportChanged: () -> Unit) {
        if (running) return
        running = true
        onChanged = onTransportChanged
        lastInterface = readActiveInterface()
        executor.scheduleWithFixedDelay(::poll, 1, 1, TimeUnit.SECONDS)
    }

    actual fun stop() {
        if (!running) return
        running = false
        onChanged = null
        executor.shutdownNow()
    }

    actual fun currentTransport(): String? = readActiveInterface()

    private fun poll() {
        if (!running) return
        val now = readActiveInterface()
        if (now != null && now != lastInterface) {
            lastInterface = now
            onChanged?.invoke()
        }
    }

    private fun readActiveInterface(): String? =
        try {
            NetworkInterface.getNetworkInterfaces()?.toList()
                ?.firstOrNull { it.isUp && !it.isLoopback && !it.isVirtual }
                ?.name
        } catch (_: Exception) {
            null
        }
}
