/*
 * The transport half of Listen Together, written against Metrolist's server
 * (MetrolistGroup/metroserver, GPL-3.0 — the same licence as this project) so that SimpMusic
 * clients share rooms with Metrolist clients.
 *
 * Every constant below that names a wire behaviour is read from that server rather than chosen:
 * see the companion object for where each one comes from. Guessing any of them produces a client
 * that connects and then silently never joins.
 */
package org.simpmusic.listentogether

import com.maxrave.ktorext.getEngine
import com.maxrave.logger.Logger
import io.ktor.client.HttpClient
import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.client.plugins.websocket.webSocketSession
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.readBytes
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.coroutines.CoroutineContext
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

private const val TAG = "ListenTogetherClient"

/**
 * What the socket reports upward.
 *
 * Everything above this class reacts to these and never to frames, which is what lets the state
 * machine in the next layer stay platform-free and testable without a socket.
 */
sealed interface ListenTogetherEvent {
    /**
     * The handshake completed and the connection is usable.
     *
     * [resumed] means a `reconnect` was sent with a stored session token — the room may still be
     * the one we were in. It is NOT a promise: the server answers `reconnected` on success and
     * `error` when the token has expired, and only that answer settles it.
     */
    data class Connected(
        val serverVersion: String,
        val compressionEnabled: Boolean,
        val resumed: Boolean,
    ) : ListenTogetherEvent

    /** One decoded protocol message. [payload] is null for types this client does not model. */
    data class Message(
        val type: String,
        val payload: Any?,
    ) : ListenTogetherEvent

    /** The first pong landed, so [ListenTogetherClient.positionAt] now returns corrected values. */
    data object ClockReady : ListenTogetherEvent

    /**
     * A pong with optional server-authoritative room state.
     *
     * Emitted on every pong, whether or not [authoritativeTrackId] is populated. Consumers should
     * check [authoritativeTrackId].isNotEmpty() before trusting the playback fields — older servers
     * leave them at their zero defaults.
     */
    data class Pong(
        val authoritativeTrackId: String,
        val authoritativeIsPlaying: Boolean,
        val authoritativePosition: Long,
        val authoritativeServerTime: Long,
    ) : ListenTogetherEvent

    /**
     * The socket is down. [willRetry] false means this client has given up and the user has to
     * rejoin by hand — the retry budget is spent, or the server refused this client outright.
     */
    data class Disconnected(
        val reason: String?,
        val willRetry: Boolean,
    ) : ListenTogetherEvent
}

/**
 * Owns one WebSocket to a Listen Together server: the capability handshake, the frame loop, the
 * ping loop that calibrates [ServerClock], and reconnection with a stored session token.
 *
 * It deliberately holds no room state — that belongs to the layer above. The one piece of protocol
 * knowledge it does keep is the session token, because reconnection has to be transparent: it is
 * read out of `room_created` / `join_approved` as those pass through, and replayed on the next
 * connection.
 */
class ListenTogetherClient(
    private val clientVersion: String,
    /**
     * Read on every connection attempt rather than captured once, so changing the server in
     * settings takes effect on the next connect instead of needing the screen to be recreated.
     */
    private val serverUrl: () -> String = { DEFAULT_SERVER_URL },
    /**
     * MUST be monotonic — [ServerClock] compares it against itself. The default is Kotlin's own
     * monotonic source, which needs no expect/actual; a platform with a better one (Android's
     * `SystemClock.elapsedRealtime`, which also counts deep sleep) can pass it instead.
     */
    private val elapsedRealtime: () -> Long = {
        PROCESS_START.elapsedNow().inWholeMilliseconds
    },
    dispatcher: CoroutineContext = Dispatchers.Default,
) : CoroutineScope {
    override val coroutineContext: CoroutineContext = SupervisorJob() + dispatcher

    private val client: HttpClient =
        HttpClient(getEngine()) {
            install(WebSockets) {
                // Keep the transport alive through Render.com's CDN/proxy idle timeout.
                // Without this the connection is severed after ~10 s of inactivity at the WebSocket
                // layer. We must send a ping *more often* than the 10s timeout, e.g. every 8s.
                pingIntervalMillis = 8000L
            }
        }

    private val serverClock = ServerClock(elapsedRealtime)

    // SUSPEND rather than a dropping overflow policy, and a buffer far larger than any burst the
    // server can produce: losing one protocol frame is a silent desync, which is strictly worse
    // than briefly slowing the reader. The server's own send buffer is 256.
    private val _events = MutableSharedFlow<ListenTogetherEvent>(extraBufferCapacity = 256)
    val events: SharedFlow<ListenTogetherEvent> = _events.asSharedFlow()

    /**
     * Reconnect countdown for the UI. A null value means no active reconnect; a non-null value
     * is the remaining milliseconds in the 5-minute budget. The UI uses this to render a
     * "Reconnecting..." indicator with a countdown, and to disable the manual reconnect button
     * while the budget is in its first attempts.
     */
    private val _reconnectState = MutableStateFlow<ReconnectState>(ReconnectState.Idle)
    val reconnectState: StateFlow<ReconnectState> = _reconnectState.asStateFlow()

    private var codec = MessageCodec(compressionEnabled = true)
    private var session: DefaultClientWebSocketSession? = null
    private var connectionJob: Job? = null
    private var handshake: CompletableDeferred<ServerCapabilities>? = null

    private var sessionToken: String? = null
    private var pingSequence = 0L
    private var reconnectDelay = INITIAL_RECONNECT_DELAY
    private var reconnectAttempts = 0
    /** Monotonic timestamp (ms) at which the current reconnect budget started. */
    private var reconnectBudgetStartMs: Long = 0
    /** Set when the server tells us retrying cannot help — see [MessageTypes.ERROR] handling. */
    private var fatal = false

    /** True between a completed handshake and the socket going down. */
    val isConnected: Boolean
        get() = session?.isActive == true && handshake?.isCompleted == true

    /** Server wall time now, or null until the first pong has landed. */
    fun serverNow(): Long? = serverClock.now()

    /** See [ServerClock.positionAt]. Falls back to [position] whenever the clock cannot help. */
    fun positionAt(
        position: Long,
        effectiveAtServerTime: Long?,
        isPlaying: Boolean,
    ): Long = serverClock.positionAt(position, effectiveAtServerTime, isPlaying)

    /**
     * Opens the connection, or does nothing if one is already open or opening.
     *
     * Reconnection is automatic from here on; callers do not call this again after a drop.
     *
     * The 5-minute reconnect budget is reset on a new connect() call. If the user calls connect()
     * after a prior budget already elapsed, the server's room will already be gone and the first
     * attempt will surface that error — the budget does not refund itself retroactively.
     */
    fun connect() {
        // Reset time budget and UI state. The attempt counter is kept across resets but it is only
        // used for the initial rapid-retries window (1s + 2s + 4s + 8s + 16s); after that the
        // budget-driven delay dominates and the attempt count is irrelevant.
        reconnectAttempts = 0
        reconnectDelay = INITIAL_RECONNECT_DELAY
        reconnectBudgetStartMs = elapsedRealtime()
        _reconnectState.value = ReconnectState.BudgetRunning(MAX_RECONNECT_BUDGET_MS)
        fatal = false
        startConnection()
    }

    private fun startConnection() {
        if (connectionJob?.isActive == true) {
            Logger.i(TAG, "Connect ignored — a connection is already active")
            return
        }
        connectionJob =
            launch {
                try {
                    openAndRun()
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    Logger.e(TAG, "Connection error: ${e.stackTraceToString()}")
                }
                // Reached on every close, clean or not — openAndRun returns when the reader stops.
                scheduleReconnect()
            }
    }

    /**
     * Sends one message. Returns false when there is no live socket, which the caller must treat
     * as "not sent" rather than retrying blindly: the reconnect path replays nothing.
     */
    suspend fun send(
        msgType: String,
        payload: Any? = null,
    ): Boolean {
        val live = session ?: return false
        if (!live.isActive) return false
        return runCatching {
            live.send(Frame.Binary(true, codec.encode(msgType, payload)))
            true
        }.getOrElse { e ->
            Logger.w(TAG, "Send failed for $msgType: ${e.message}")
            false
        }
    }

    /** Leaves for good: stops retrying, forgets the session token, closes the socket. */
    fun disconnect() {
        Logger.i(TAG, "Disconnecting by request")
        sessionToken = null
        reconnectAttempts = 0
        reconnectBudgetStartMs = 0
        _reconnectState.value = ReconnectState.Idle
        connectionJob?.cancel()
        connectionJob = null
        launch {
            runCatching { session?.close() }
            session = null
        }
    }

    /**
     * Force a reconnect now, ignoring the current backoff delay.
     *
     * Called by the network monitor when the transport type changes (Wi-Fi → cellular, etc.).
     * The budget is NOT reset — a handoff is not a new session, it is a continuation of the
     * same room, so the server's reconnect grace period still applies.
     */
    fun forceReconnect() {
        if (connectionJob?.isActive == true) {
            Logger.i(TAG, "Force reconnect requested — cancelling current attempt")
            connectionJob?.cancel()
            connectionJob = null
        }
        startConnection()
    }

    /** Releases the HTTP client too. The instance is unusable afterwards. */
    fun release() {
        disconnect()
        runCatching { client.close() }
        cancel()
    }

    private suspend fun openAndRun() {
        val url = serverUrl().ifBlank { DEFAULT_SERVER_URL }
        Logger.i(TAG, "Connecting to $url")
        val live = client.webSocketSession(url)
        session = live
        serverClock.reset()
        val pending = CompletableDeferred<ServerCapabilities>()
        handshake = pending

        try {
            coroutineScope {
                val reader = launch { readFrames(live) }

                // The handshake is an ordinary message, so it can only be answered once the reader
                // is running — hence the reader starts first and completes `pending` from there.
                send(
                    MessageTypes.CLIENT_CAPABILITIES,
                    ClientCapabilities(
                        // The server rejects a client that says false, with `unsupported_client`.
                        supportsProtobuf = true,
                        supportsCompression = true,
                        clientVersion = clientVersion,
                    ),
                )
                val caps =
                    withTimeoutOrNull(HANDSHAKE_TIMEOUT) { pending.await() }
                        ?: throw IllegalStateException("Handshake timed out after $HANDSHAKE_TIMEOUT")

                // Only narrows: a server that cannot inflate must not be sent gzip. Widening is
                // pointless because the 100-byte threshold already suppresses useless compression.
                if (!caps.supportsCompression) {
                    Logger.w(TAG, "Server reports no compression support — sending uncompressed")
                    codec = MessageCodec(compressionEnabled = false)
                }

                val token = sessionToken
                Logger.i(TAG, "Handshake complete. Current sessionToken=$token")
                if (token != null) {
                    Logger.i(TAG, "Resuming with stored session token: $token")
                    send(MessageTypes.RECONNECT, ReconnectPayload(sessionToken = token))
                }

                // A completed handshake is what "connected" means, so the budget refills here and
                // not merely when the socket opens — a server that accepts TCP but never answers
                // must not buy itself unlimited retries.
                reconnectDelay = INITIAL_RECONNECT_DELAY
                reconnectAttempts = 0
                _reconnectState.value = ReconnectState.Idle
                _events.emit(
                    ListenTogetherEvent.Connected(
                        serverVersion = caps.serverVersion,
                        compressionEnabled = caps.supportsCompression,
                        resumed = token != null,
                    ),
                )

                val pinger = launch { pingLoop() }
                reader.join()
                pinger.cancel()
            }
        } finally {
            handshake = null
            session = null
            // NonCancellable because this runs on the disconnect() path too, where the job has
            // already been cancelled — and close() is itself a suspend call, so without this it
            // would abort immediately and the close frame would never be sent.
            withContext(NonCancellable) { runCatching { live.close() } }
        }
    }

    private suspend fun readFrames(live: DefaultClientWebSocketSession) {
        for (frame in live.incoming) {
            // Metrolist's server writes BinaryMessage exclusively; anything else is not ours.
            if (frame !is Frame.Binary) continue
            // Not `getOrElse { continue }`: Kotlin still forbids break/continue inside a lambda,
            // even an inlined one, so the failure has to be unwrapped before the loop can skip.
            val bytes = frame.readBytes()
            val decoded = runCatching { codec.decode(bytes) }.getOrNull()
            if (decoded == null) {
                Logger.w(TAG, "Undecodable frame dropped (${bytes.size} bytes)")
                continue
            }
            val (type, payloadBytes) = decoded
            val payload = codec.decodePayload(type, payloadBytes)
            handleMessage(type, payload)
        }
        Logger.i(TAG, "Read loop ended; socket closed")
    }

    private suspend fun handleMessage(
        type: String,
        payload: Any?,
    ) {
        when (type) {
            MessageTypes.SERVER_CAPABILITIES -> {
                val caps = payload as? ServerCapabilities
                if (caps == null) {
                    Logger.w(TAG, "Malformed server_capabilities")
                    return
                }
                Logger.i(TAG, "Handshake complete — server version ${caps.serverVersion}")
                handshake?.complete(caps)
                // Internal to the handshake; the layer above has no use for it.
                return
            }

            MessageTypes.PONG -> {
                val pong = payload as? PongPayload ?: return
                val firstSample =
                    serverClock.recordPong(
                        clientTime = pong.clientTime,
                        serverReceiveTime = pong.serverReceiveTime,
                        serverSendTime = pong.serverSendTime,
                    )
                if (firstSample) {
                    Logger.i(TAG, "Server clock calibrated")
                    _events.emit(ListenTogetherEvent.ClockReady)
                }
                // Always emit the pong so the bridge can use authoritative fields when present.
                _events.emit(
                    ListenTogetherEvent.Pong(
                        authoritativeTrackId = pong.authoritativeTrackId,
                        authoritativeIsPlaying = pong.authoritativeIsPlaying,
                        authoritativePosition = pong.authoritativePosition,
                        authoritativeServerTime = pong.authoritativeServerTime,
                    ),
                )
                return
            }

            // The token is what makes a drop survivable, so it is captured in passing rather than
            // asked of the layer above — which may not have been listening yet.
            MessageTypes.ROOM_CREATED -> (payload as? RoomCreatedPayload)?.let { sessionToken = it.sessionToken }
            MessageTypes.JOIN_APPROVED -> (payload as? JoinApprovedPayload)?.let { sessionToken = it.sessionToken }

            MessageTypes.ERROR -> {
                val error = payload as? ErrorPayload
                Logger.w(TAG, "Server error: ${error?.code} ${error?.message}")
                if (error != null && error.code in setOf("session_expired", "session_not_found", "room_not_found", "room_closed", "not_in_room")) {
                    sessionToken = null
                }
                if (error != null && error.code in NON_RECOVERABLE_ERROR_CODES) {
                    Logger.e(TAG, "Server rejected this client (${error.code}) — not retrying")
                    fatal = true
                }
            }

            // A room we can no longer return to: the token is spent either way.
            MessageTypes.KICKED -> sessionToken = null
        }
        _events.emit(ListenTogetherEvent.Message(type, payload))
    }

    fun clearSessionToken() {
        Logger.i(TAG, "clearSessionToken called")
        sessionToken = null
    }

    /**
     * Feeds [ServerClock] and doubles as a liveness signal.
     *
     * The first few pings are close together because the clock is weighted, not averaged: one
     * sample sets the offset and each later one moves it by at most a quarter, so a room joined
     * during the first seconds would otherwise seek against a barely-calibrated clock. After that
     * the interval opens up — the server's read deadline is 60s and any message refreshes it, so
     * [PING_INTERVAL] is also what keeps an idle room's socket alive.
     */
    private suspend fun pingLoop() {
        var sent = 0
        while (true) {
            val ok =
                send(
                    MessageTypes.PING,
                    PingPayload(clientTime = elapsedRealtime(), sequence = ++pingSequence),
                )
            if (!ok) return
            sent++
            delay(if (sent < CALIBRATION_PINGS) CALIBRATION_PING_INTERVAL else PING_INTERVAL)
        }
    }

    private suspend fun scheduleReconnect() {
        if (fatal) {
            _reconnectState.value = ReconnectState.Fatal
            _events.emit(ListenTogetherEvent.Disconnected("Server rejected this client", willRetry = false))
            return
        }

        // Calculate elapsed time since budget started
        val nowMs = elapsedRealtime()
        val elapsedMs = nowMs - reconnectBudgetStartMs

        // If budget expired, emit fatal and stop retrying
        if (elapsedMs >= RECONNECT_GRACE_MS) {
            Logger.w(TAG, "Reconnect budget expired after ${elapsedMs / 1000}s")
            // The token is dropped with the budget: a later connect() is a fresh join, not a
            // resume, and replaying a token the server may already have expired would be answered
            // with an error rather than a room.
            sessionToken = null
            _reconnectState.value = ReconnectState.Fatal
            _events.emit(
                ListenTogetherEvent.Disconnected(
                    reason = "Could not reconnect within time budget",
                    willRetry = false,
                ),
            )
            return
        }

        // Compute remaining budget and derive a delay from it.
        // We keep the old attempt-based scheme for the first few tries (1,2,4,8,16s) to handle
        // transient blips, then switch to a linear spread of the remaining budget.
        reconnectAttempts++
        if (reconnectAttempts <= 5) {
            // Use exponential backoff for first 5 attempts: 1s,2s,4s,8s,16s capped at MAX_RECONNECT_DELAY
            // 2.0.pow(x) computes 2^x
            reconnectDelay = (INITIAL_RECONNECT_DELAY * 2.0.pow(reconnectAttempts - 1)).coerceAtMost(MAX_RECONNECT_DELAY)
        } else {
            // After attempt 5, spread remaining budget linearly so that we use the full window
            // This attempts to make at least one reconnect try every 5-30s depending on remaining time
            val remainingMs = RECONNECT_GRACE_MS - elapsedMs
            // Aim for at least 5 retries in the remaining time, min 1s between tries
            reconnectDelay = maxOf((remainingMs / 5).milliseconds, INITIAL_RECONNECT_DELAY).coerceAtMost(MAX_RECONNECT_DELAY)
        }

        Logger.i(TAG, "Reconnecting in $reconnectDelay (attempt $reconnectAttempts, ${(RECONNECT_GRACE_MS - elapsedMs) / 1000}s budget left)")
        _events.emit(ListenTogetherEvent.Disconnected("Connection lost", willRetry = true))

        _reconnectState.value = ReconnectState.BudgetRunning(RECONNECT_GRACE_MS - elapsedMs)

        delay(reconnectDelay)
        connectionJob = null
        startConnection()
    }

    companion object {
        /**
         * The one public server, run by "Nyx" and already shared by several Metrolist forks.
         * Rooms are only shared by clients pointed at the SAME server, so this default is what
         * makes SimpMusic interoperable out of the box.
         */
        const val DEFAULT_SERVER_URL = "wss://metroserver-vjux.onrender.com/ws"

        /** Anchors the default monotonic source; see [elapsedRealtime]. */
        private val PROCESS_START = TimeSource.Monotonic.markNow()

        private val HANDSHAKE_TIMEOUT = 10.seconds

        /** Metroserver `client.go`: the read deadline is 60s and any inbound message refreshes it. */
        private val PING_INTERVAL = 15.seconds
        private val CALIBRATION_PING_INTERVAL = 1.seconds
        private const val CALIBRATION_PINGS = 3

        /** Maximum time budget for reconnection in milliseconds (5 minutes). */
        private val MAX_RECONNECT_BUDGET_MS = 5.minutes.inWholeMilliseconds

        /**
         * The server's own reconnect grace period — matches this so the client does not retry
         * past the point where the server drops the room. After this budget elapses, the client
         * emits fatal and the user must rejoin by hand.
         *
         * Counted milliseconds since first reconnect attempt, not raw clock time. The budget
         * refills only on a COMPLETED handshake, so a server that accepts the socket and then
         * goes quiet still exhausts it. This is intentionally slightly shorter than the server's
         * ReconnectGracePeriod to let the client surface the error before the server reaps the room.
         */
        private val RECONNECT_GRACE_MS = 4.minutes.inWholeMilliseconds

        /**
         * Initial backoff between the first and second attempt, and the minimum interval between
         * any two retry attempts. The full budget is 5 minutes; the first few attempts use short
         * delays, and the remaining budget is spread over later attempts with exponential backoff
         * capped at 16 seconds.
         */
        private val INITIAL_RECONNECT_DELAY: Duration = 1.seconds
        private val MAX_RECONNECT_DELAY: Duration = 16.seconds

        /**
         * Errors where reconnecting repeats the same rejection. Everything else the server sends —
         * `rate_limited`, `invalid_payload`, `unknown_message_type` — is about one message, not the
         * connection, and must NOT stop retrying.
         */
        private val NON_RECOVERABLE_ERROR_CODES = setOf("unsupported_client")
    }
}

/**
 * State of the reconnection budget, exposed to the UI layer.
 */
sealed interface ReconnectState {
    /** No active reconnect; either connected or disconnected by user request. */
    object Idle : ReconnectState

    /** Reconnecting within the 5-minute budget. [remainingMs] is the time left. */
    data class BudgetRunning(val remainingMs: Long) : ReconnectState

    /** Budget exhausted or server rejected — user must act. */
    object Fatal : ReconnectState
}
