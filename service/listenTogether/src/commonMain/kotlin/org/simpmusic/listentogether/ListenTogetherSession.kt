package org.simpmusic.listentogether

import com.maxrave.logger.Logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.seconds

private const val TAG = "ListenTogetherSession"

/** Where the socket is, independent of whether a room has been joined. */
sealed interface ConnectionState {
    data object Disconnected : ConnectionState

    data object Connecting : ConnectionState

    data class Connected(val serverVersion: String) : ConnectionState

    /** Retries are exhausted or the server refused us; the user has to act. */
    data class Failed(val reason: String) : ConnectionState
}

/** One person in the room, as the UI needs them. */
data class RoomMember(
    val userId: String,
    val username: String,
    val isHost: Boolean,
    val isConnected: Boolean,
    val avatarUrl: String? = null,
    /** True while this member has not answered `buffer_ready` for the current track. */
    val isBuffering: Boolean = false,
)

data class PendingJoin(
    val userId: String,
    val username: String,
    val avatarUrl: String? = null,
)

data class PendingSuggestion(
    val suggestionId: String,
    val fromUsername: String,
    val track: TrackInfo,
)

data class JamPermissionsState(
    val allowQueue: Boolean = true,
    val allowReorder: Boolean = false,
    val allowPlayDirect: Boolean = false,
    val allowSeek: Boolean = false,
    val allowPlayPause: Boolean = false,
)

data class ChatMessageState(
    val id: String,
    val senderId: String,
    val senderName: String,
    val senderAvatar: String? = null,
    val text: String,
    val timestamp: Long,
    val replyToId: String? = null,
    val replyToText: String? = null,
    val replyToSenderName: String? = null,
    val reactions: List<String> = emptyList(),
)

data class ListenTogetherState(
    val connection: ConnectionState = ConnectionState.Disconnected,
    /** Null until a room is created or joined. */
    val roomCode: String? = null,
    val selfUserId: String = "",
    val isHost: Boolean = false,
    val members: List<RoomMember> = emptyList(),
    val joinRequests: List<PendingJoin> = emptyList(),
    val suggestions: List<PendingSuggestion> = emptyList(),
    val currentTrack: TrackInfo? = null,
    val isPlaying: Boolean = false,
    val position: Long = 0L,
    val permissions: JamPermissionsState = JamPermissionsState(),
    val chatMessages: List<ChatMessageState> = emptyList(),
    val queue: List<TrackInfo> = emptyList(),
    /** Non-empty while the room is held at the buffer barrier. */
    val waitingFor: List<String> = emptyList(),
    /** Server time the last transport command was captured at; 0 when unknown. */
    val lastActionServerTime: Long = 0L,
    val pendingJoinCode: String? = null,
    val error: String? = null,
) {
    val inRoom: Boolean get() = roomCode != null
    val isConnected: Boolean get() = connection is ConnectionState.Connected

    /** Names, not ids — "Waiting for Long" is the only useful phrasing of the barrier. */
    val waitingForNames: List<String>
        get() = waitingFor.mapNotNull { id -> members.firstOrNull { it.userId == id }?.username }
}

/**
 * The room state machine.
 *
 * Consumes [ListenTogetherClient.events] and never touches a frame, which is what keeps it
 * testable without a socket and identical on Android and Desktop — the two platforms run entirely
 * separate player handlers, so anything that reached into playback from here would have to be
 * written twice.
 */
class ListenTogetherSession(
    private val client: ListenTogetherClient,
    dispatcher: CoroutineContext = Dispatchers.Default,
) : CoroutineScope {
    override val coroutineContext: CoroutineContext = SupervisorJob() + dispatcher

    private val _state = MutableStateFlow(ListenTogetherState())
    val state: StateFlow<ListenTogetherState> = _state.asStateFlow()

    /**
     * Authoritative playback state from the server's pong response.
     *
     * Only emitted when the pong carries non-empty `authoritativeTrackId` (i.e. the server has
     * been updated to include room state in pong). Subscribers should check the track ID before
     * acting. On stock metroserver this flow never emits.
     */
    private val _authoritativePong = MutableSharedFlow<ListenTogetherEvent.Pong>(extraBufferCapacity = 4)
    val authoritativePong: SharedFlow<ListenTogetherEvent.Pong> = _authoritativePong.asSharedFlow()

    private var pump: Job? = null

    /**
     * A track change is two protocol frames: CHANGE_TRACK clears the server's playing flag, then
     * PLAY or PAUSE restores the intent. Keep that pair adjacent to every other playback frame.
     */
    private val playbackActionMutex = Mutex()

    /** Host-side conveniences from settings; both default off, matching the design's toggles. */
    var autoApproveJoins: Boolean = false
    var autoApproveSuggestions: Boolean = false
    var isBlockedUser: (String) -> Boolean = { false }

    /** Opens the socket. Joining a room is a separate, later step. */
    fun connect() {
        if (pump == null) {
            pump = launch { client.events.collect(::onEvent) }
        }
        _state.update { it.copy(connection = ConnectionState.Connecting, error = null) }
        client.connect()
    }

    fun disconnect() {
        client.disconnect()
        _state.value = ListenTogetherState()
    }

    private var pendingUsername: String = ""
    private var pendingAvatar: String? = null

    fun createRoom(
        username: String,
        avatarUrl: String? = null,
    ) = launch {
        pendingUsername = username.trim()
        pendingAvatar = avatarUrl?.trim()?.ifBlank { null }
        val wireUsername = if (pendingAvatar != null) "$pendingUsername\u001f$pendingAvatar" else pendingUsername
        if (_state.value.connection !is ConnectionState.Connected) {
            connect()
            val connected = withTimeoutOrNull(20.seconds) {
                _state.first { it.connection is ConnectionState.Connected }
            }
            if (connected == null) {
                _state.update { it.copy(error = "Could not connect to Jam server") }
                return@launch
            }
        }
        client.send(MessageTypes.CREATE_ROOM, CreateRoomPayload(username = wireUsername))
    }

    fun joinRoom(
        roomCode: String,
        username: String,
        avatarUrl: String? = null,
    ) = launch {
        pendingUsername = username.trim()
        pendingAvatar = avatarUrl?.trim()?.ifBlank { null }
        val code = roomCode.trim().uppercase()
        val wireUsername = if (pendingAvatar != null) "$pendingUsername\u001f$pendingAvatar" else pendingUsername
        _state.update { it.copy(pendingJoinCode = code, error = null) }
        if (_state.value.connection !is ConnectionState.Connected) {
            connect()
            val connected = withTimeoutOrNull(20.seconds) {
                _state.first { it.connection is ConnectionState.Connected }
            }
            if (connected == null) {
                _state.update { it.copy(pendingJoinCode = null, error = "Could not connect to Jam server") }
                return@launch
            }
        }
        val sent = client.send(MessageTypes.JOIN_ROOM, JoinRoomPayload(roomCode = code, username = wireUsername))
        if (!sent) {
            _state.update { it.copy(pendingJoinCode = null, error = "Failed to send join request") }
        }
    }

    /** Gives up on a join that has not been answered. Local only — the server needs no message. */
    fun cancelJoin() = _state.update { it.copy(pendingJoinCode = null) }

    fun leaveRoom() =
        launch {
            client.send(MessageTypes.LEAVE_ROOM, LeaveRoomPayload())
            client.clearSessionToken()
            // The server sends nothing back for leave_room, so the local state is cleared here —
            // waiting for a confirmation that never arrives would leave the room UI on screen.
            _state.update {
                it.copy(
                    roomCode = null,
                    isHost = false,
                    members = emptyList(),
                    joinRequests = emptyList(),
                    suggestions = emptyList(),
                    currentTrack = null,
                    queue = emptyList(),
                    chatMessages = emptyList(),
                    waitingFor = emptyList(),
                )
            }
        }

    fun sendChatMessage(
        text: String,
        replyToId: String? = null,
        replyToText: String? = null,
        replyToSenderName: String? = null,
    ) = launch {
        if (text.isBlank()) return@launch
        val (name, avatar) = parseUserAndAvatar(pendingUsername)
        val nowMs = client.serverNow() ?: kotlin.time.TimeSource.Monotonic.markNow().elapsedNow().inWholeMilliseconds
        val msgId = "msg_${nowMs}_${_state.value.selfUserId.take(4)}"
        val senderAvatar = pendingAvatar ?: avatar.orEmpty()
        val payload =
            ChatMessagePayload(
                id = msgId,
                senderId = _state.value.selfUserId,
                senderName = name,
                senderAvatar = senderAvatar,
                text = text.trim(),
                timestamp = nowMs,
                replyToId = replyToId.orEmpty(),
                replyToText = replyToText.orEmpty(),
                replyToSenderName = replyToSenderName.orEmpty(),
            )
        client.send(MessageTypes.CHAT, payload)
    }

    fun reactToChatMessage(
        messageId: String,
        emoji: String,
    ) = launch {
        val msg = _state.value.chatMessages.firstOrNull { it.id == messageId } ?: return@launch
        val currentReactions = msg.reactions.toMutableList()
        if (emoji in currentReactions) {
            currentReactions.remove(emoji)
        } else {
            currentReactions.add(emoji)
        }
        val nowMs = client.serverNow() ?: kotlin.time.TimeSource.Monotonic.markNow().elapsedNow().inWholeMilliseconds
        val payload =
            ChatMessagePayload(
                id = messageId,
                senderId = _state.value.selfUserId,
                text = "",
                timestamp = nowMs,
                reactions = currentReactions,
            )
        client.send(MessageTypes.CHAT, payload)
    }

    fun approveJoin(userId: String) =
        launch {
            client.send(MessageTypes.APPROVE_JOIN, ApproveJoinPayload(userId = userId))
            _state.update { s -> s.copy(joinRequests = s.joinRequests.filterNot { it.userId == userId }) }
        }

    fun rejectJoin(userId: String) =
        launch {
            client.send(MessageTypes.REJECT_JOIN, RejectJoinPayload(userId = userId))
            _state.update { s -> s.copy(joinRequests = s.joinRequests.filterNot { it.userId == userId }) }
        }

    fun approveSuggestion(suggestionId: String) =
        launch {
            client.send(MessageTypes.APPROVE_SUGGESTION, ApproveSuggestionPayload(suggestionId = suggestionId))
            dropSuggestion(suggestionId)
        }

    fun rejectSuggestion(suggestionId: String) =
        launch {
            client.send(MessageTypes.REJECT_SUGGESTION, RejectSuggestionPayload(suggestionId = suggestionId))
            dropSuggestion(suggestionId)
        }

    fun kickUser(userId: String) =
        launch {
            _state.update { s -> s.copy(members = s.members.filterNot { it.userId == userId }) }
            client.send(MessageTypes.KICK_USER, KickUserPayload(userId = userId, reason = "Removed by host"))
        }

    fun transferHost(userId: String) =
        launch { client.send(MessageTypes.TRANSFER_HOST, TransferHostPayload(newHostId = userId)) }

    fun suggestTrack(track: TrackInfo) =
        launch { client.send(MessageTypes.SUGGEST_TRACK, SuggestTrackPayload(trackInfo = track)) }

    fun playTrackDirect(track: TrackInfo) =
        launch {
            sendTrackChangeAndRestoreIntent(
                trackId = track.id,
                position = 0L,
                trackInfo = track,
                shouldPlay = true,
            )
        }

    fun playQueuedTrack(index: Int, track: TrackInfo) =
        launch {
            val currentQueue = _state.value.queue.toMutableList()
            val updatedQueue =
                if (index in currentQueue.indices) {
                    currentQueue.removeAt(index)
                    currentQueue
                } else {
                    currentQueue.filterNot { it.id == track.id }
                }
            // Capture the room's playback intent before the track change. The server's CHANGE_TRACK
            // handler resets isPlaying=false, so we must re-publish the intent on the same frame.
            // This is what makes auto-advance (and manual NEXT) actually resume playback: a track
            // change without a follow-up PLAY/PAUSE leaves the new track permanently paused on
            // every client, including the host.
            val wasPlaying = _state.value.isPlaying
            _state.update { it.copy(queue = updatedQueue) }
            sendTrackChangeAndRestoreIntent(
                trackId = track.id,
                position = 0L,
                trackInfo = track,
                queue = updatedQueue,
                queueTitle = formatJamPermissions(_state.value.permissions),
                shouldPlay = wasPlaying,
            )
        }

    fun play() = launch {
        val currentTrack = _state.value.currentTrack
        if (currentTrack != null) {
            sendPlaybackAction(
                action = PlaybackActions.PLAY,
                trackId = currentTrack.id,
                position = _state.value.position,
                trackInfo = null,
            )
        }
    }

    fun pause() = launch {
        val currentTrack = _state.value.currentTrack
        if (currentTrack != null) {
            sendPlaybackAction(
                action = PlaybackActions.PAUSE,
                trackId = currentTrack.id,
                position = _state.value.position,
                trackInfo = null,
            )
        }
    }

    fun seekTo(position: Long) = launch {
        val currentTrack = _state.value.currentTrack
        if (currentTrack != null) {
            sendPlaybackAction(
                action = PlaybackActions.SEEK,
                trackId = currentTrack.id,
                position = position,
                trackInfo = null,
            )
        }
    }

    /**
     * Publishes one transport command to the room. Host only — the server ignores it from a guest.
     *
     * `capturedAtServerTime` is what lets a late-arriving PLAY still land on the right position:
     * the receiver advances [position] by however long the frame spent in flight, measured on the
     * SERVER's clock rather than its own. Sending 0 (an uncalibrated clock) is safe — the receiver
     * treats it as "unknown" and uses the raw position.
     */
    fun sendPlaybackAction(
        action: String,
        trackId: String,
        position: Long,
        trackInfo: TrackInfo?,
        queue: List<TrackInfo> = emptyList(),
        queueTitle: String = "",
    ) = launch {
        sendPlaybackActionFrame(
            PlaybackActionPayload(
                action = action,
                trackId = trackId,
                position = position,
                trackInfo = trackInfo,
                // Carried on the SAME message as the track deliberately. Sent separately they are
                // two independent sends with no ordering guarantee, and a queue arriving after the
                // track means the guest has already committed to a one-track queue — it then plays
                // its own next song and the room splits one track later.
                queue = queue,
                queueTitle = queueTitle,
                capturedAtServerTime = client.serverNow() ?: 0L,
            ),
        )
    }

    /**
     * Publishes a track transition as one ordered transaction.
     *
     * The server clears `isPlaying` for CHANGE_TRACK, even when the outgoing song was running. The
     * follow-up transport action is therefore required for every transition — manual next, natural
     * auto-advance, and a normal player timeline transition alike.
     */
    fun sendTrackChange(
        trackId: String,
        position: Long,
        trackInfo: TrackInfo,
        queue: List<TrackInfo> = emptyList(),
        queueTitle: String = "",
        shouldPlay: Boolean,
    ) = launch {
        sendTrackChangeAndRestoreIntent(
            trackId = trackId,
            position = position,
            trackInfo = trackInfo,
            queue = queue,
            queueTitle = queueTitle,
            shouldPlay = shouldPlay,
        )
    }

    private suspend fun sendTrackChangeAndRestoreIntent(
        trackId: String,
        position: Long,
        trackInfo: TrackInfo,
        queue: List<TrackInfo> = emptyList(),
        queueTitle: String = "",
        shouldPlay: Boolean,
    ) {
        playbackActionMutex.withLock {
            val changed =
                client.send(
                    MessageTypes.PLAYBACK_ACTION,
                    PlaybackActionPayload(
                        action = PlaybackActions.CHANGE_TRACK,
                        trackId = trackId,
                        position = position,
                        trackInfo = trackInfo,
                        queue = queue,
                        queueTitle = queueTitle,
                        capturedAtServerTime = client.serverNow() ?: 0L,
                    ),
                )
            if (!changed) return

            client.send(
                MessageTypes.PLAYBACK_ACTION,
                PlaybackActionPayload(
                    action = if (shouldPlay) PlaybackActions.PLAY else PlaybackActions.PAUSE,
                    // Empty lets the server use the track it just committed, avoiding stale_track.
                    trackId = "",
                    position = position,
                    trackInfo = null,
                    capturedAtServerTime = client.serverNow() ?: 0L,
                ),
            )
        }
    }

    private suspend fun sendPlaybackActionFrame(payload: PlaybackActionPayload): Boolean =
        playbackActionMutex.withLock { client.send(MessageTypes.PLAYBACK_ACTION, payload) }

    /** Publishes the whole queue. Host only; guests take the host's order verbatim. */
    fun sendQueue(
        tracks: List<TrackInfo>,
        queueTitle: String = "",
    ) = launch {
        _state.update { it.copy(queue = tracks) }
        val title = queueTitle.ifBlank { formatJamPermissions(_state.value.permissions) }
        sendPlaybackActionFrame(
            PlaybackActionPayload(
                action = PlaybackActions.SYNC_QUEUE,
                queue = tracks,
                queueTitle = title,
                capturedAtServerTime = client.serverNow() ?: 0L,
            ),
        )
    }

    fun updateJamPermissions(perms: JamPermissionsState) = launch {
        _state.update { it.copy(permissions = perms) }
        if (_state.value.inRoom && _state.value.isHost) {
            sendPlaybackActionFrame(
                PlaybackActionPayload(
                    action = PlaybackActions.SYNC_QUEUE,
                    queue = _state.value.queue,
                    queueTitle = formatJamPermissions(perms),
                    capturedAtServerTime = client.serverNow() ?: 0L,
                ),
            )
        }
    }

    fun addToQueue(track: TrackInfo) = launch {
        val currentQueue = _state.value.queue
        if (_state.value.isHost || _state.value.permissions.allowQueue) {
            sendPlaybackActionFrame(
                PlaybackActionPayload(
                    action = PlaybackActions.QUEUE_ADD,
                    trackInfo = track,
                    queueTitle = formatJamPermissions(_state.value.permissions),
                    capturedAtServerTime = client.serverNow() ?: 0L,
                ),
            )
        } else {
            suggestTrack(track)
        }
    }

    fun removeQueueItem(index: Int) = launch {
        val currentQueue = _state.value.queue.toMutableList()
        if (index in currentQueue.indices) {
            val item = currentQueue.removeAt(index)
            if (_state.value.isHost || _state.value.permissions.allowReorder) {
                sendQueue(currentQueue)
            }
        }
    }

    fun reorderQueue(fromIndex: Int, toIndex: Int) = launch {
        val currentQueue = _state.value.queue.toMutableList()
        if (fromIndex in currentQueue.indices && toIndex in currentQueue.indices) {
            val item = currentQueue.removeAt(fromIndex)
            currentQueue.add(toIndex, item)
            if (_state.value.isHost || _state.value.permissions.allowReorder) {
                sendQueue(currentQueue)
            }
        }
    }

    fun endRoom() = launch {
        if (_state.value.inRoom && _state.value.isHost) {
            // Kick all other members first
            _state.value.members.filterNot { it.isHost }.forEach { member ->
                client.send(MessageTypes.KICK_USER, KickUserPayload(userId = member.userId, reason = "Host ended the Jam"))
            }
        }
        leaveRoom()
    }

    /** See `ServerClock.positionAt` — corrects a room position for time spent in flight. */
    fun positionAt(
        position: Long,
        isPlaying: Boolean,
    ): Long = client.positionAt(position, _state.value.lastActionServerTime, isPlaying)

    /**
     * Variant that accepts an explicit server timestamp — used by [pongDriftCorrection] where the
     * pong carries its own `authoritativeServerTime` which is more precise than the stored
     * `lastActionServerTime` from the last explicit room command.
     */
    fun positionAt(
        position: Long,
        effectiveAtServerTime: Long,
        isPlaying: Boolean,
    ): Long = client.positionAt(position, effectiveAtServerTime, isPlaying)

    /**
     * Asks the server for the room's current state.
     *
     * This is how a guest rejoins the room's timeline after driving its own transport: the server
     * answers `sync_state` with the live position, so pressing play lands where everyone else
     * actually is rather than where this device happened to stop.
     */
    fun requestSync() =
        launch {
            client.send(MessageTypes.REQUEST_SYNC, null)
        }

    /** Answers the buffer barrier for [trackId]; until every member does, nobody hears anything. */
    fun reportBufferReady(trackId: String) =
        launch { client.send(MessageTypes.BUFFER_READY, BufferReadyPayload(trackId = trackId)) }

    fun clearError() = _state.update { it.copy(error = null) }

    fun release() {
        client.release()
        pump = null
    }

    private fun dropSuggestion(id: String) =
        _state.update { s -> s.copy(suggestions = s.suggestions.filterNot { it.suggestionId == id }) }

    private fun onEvent(event: ListenTogetherEvent) {
        when (event) {
            is ListenTogetherEvent.Connected ->
                _state.update { it.copy(connection = ConnectionState.Connected(event.serverVersion)) }

            is ListenTogetherEvent.ClockReady -> Unit

            is ListenTogetherEvent.Pong -> {
                // Forward to bridge only when the server populates authoritative fields.
                if (event.authoritativeTrackId.isNotEmpty()) {
                    launch { _authoritativePong.emit(event) }
                }
            }

            is ListenTogetherEvent.Disconnected ->
                _state.update {
                    if (event.willRetry) {
                        it.copy(connection = ConnectionState.Connecting)
                    } else {
                        // Losing the socket loses the room with it; leaving the room UI up would
                        // offer controls that silently do nothing.
                        ListenTogetherState(connection = ConnectionState.Failed(event.reason ?: "Disconnected"))
                    }
                }

            is ListenTogetherEvent.Message -> onMessage(event.type, event.payload)
        }
    }

    private fun onMessage(
        type: String,
        payload: Any?,
    ) {
        when (type) {
            MessageTypes.ROOM_CREATED -> {
                val p = payload as? RoomCreatedPayload ?: return
                val (name, avatar) = parseUserAndAvatar(pendingUsername)
                _state.update {
                    it.copy(
                        roomCode = p.roomCode,
                        selfUserId = p.userId,
                        isHost = true,
                        // The server sends no member list for a brand-new room: the host is alone
                        // in it, and USER_JOINED carries everyone who arrives afterwards.
                        members = listOf(RoomMember(p.userId, name, isHost = true, isConnected = true, avatarUrl = pendingAvatar ?: avatar)),
                    )
                }
            }

            MessageTypes.JOIN_APPROVED -> {
                val p = payload as? JoinApprovedPayload ?: return
                // Ask for the current state explicitly: JoinApproved carries whatever the server
                // last heard, which is nothing at all if the host has not issued a command yet.
                launch { client.send(MessageTypes.REQUEST_SYNC, null) }
                _state.update {
                    it.copy(
                        roomCode = p.roomCode,
                        selfUserId = p.userId,
                        isHost = false,
                        pendingJoinCode = null,
                        members = p.state?.users.orEmpty().map { u -> u.toMember() },
                        queue = p.state?.queue.orEmpty(),
                        currentTrack = p.state?.currentTrack,
                        isPlaying = p.state?.isPlaying ?: false,
                        position = p.state?.position ?: 0L,
                    )
                }
            }

            MessageTypes.JOIN_REJECTED ->
                _state.update {
                    it.copy(
                        pendingJoinCode = null,
                        error = (payload as? JoinRejectedPayload)?.reason?.ifBlank { null } ?: "The host declined",
                    )
                }

            MessageTypes.JOIN_REQUEST -> {
                val p = payload as? JoinRequestPayload ?: return
                val (name, avatar) = parseUserAndAvatar(p.username)
                if (isBlockedUser(name)) {
                    rejectJoin(p.userId)
                    return
                }
                if (autoApproveJoins) {
                    approveJoin(p.userId)
                    return
                }
                _state.update { s ->
                    if (s.joinRequests.any { it.userId == p.userId }) {
                        s
                    } else {
                        s.copy(joinRequests = s.joinRequests + PendingJoin(p.userId, name, avatarUrl = avatar))
                    }
                }
            }

            MessageTypes.USER_JOINED -> {
                val p = payload as? UserJoinedPayload ?: return
                val (name, avatar) = parseUserAndAvatar(p.username)
                if (_state.value.isHost && isBlockedUser(name)) {
                    kickUser(p.userId)
                    return
                }
                _state.update { s ->
                    if (s.members.any { it.userId == p.userId }) {
                        s
                    } else {
                        s.copy(members = s.members + RoomMember(p.userId, name, isHost = false, isConnected = true, avatarUrl = avatar))
                    }
                }
                if (_state.value.isHost) {
                    launch {
                        kotlinx.coroutines.delay(1000)
                        sendQueue(_state.value.queue)
                    }
                }
            }

            MessageTypes.USER_LEFT -> {
                val p = payload as? UserLeftPayload ?: return
                _state.update { s -> s.copy(members = s.members.filterNot { it.userId == p.userId }) }
            }

            MessageTypes.USER_DISCONNECTED -> {
                val p = payload as? UserDisconnectedPayload ?: return
                _state.update { s -> s.copy(members = s.members.map { if (it.userId == p.userId) it.copy(isConnected = false) else it }) }
            }

            MessageTypes.USER_RECONNECTED -> {
                val p = payload as? UserReconnectedPayload ?: return
                _state.update { s -> s.copy(members = s.members.map { if (it.userId == p.userId) it.copy(isConnected = true) else it }) }
            }

            MessageTypes.HOST_CHANGED -> {
                val p = payload as? HostChangedPayload ?: return
                _state.update { s ->
                    s.copy(
                        isHost = p.newHostId == s.selfUserId,
                        members = s.members.map { it.copy(isHost = it.userId == p.newHostId) },
                    )
                }
            }

            MessageTypes.KICKED -> {
                client.clearSessionToken()
                _state.value =
                    ListenTogetherState(
                        connection = _state.value.connection,
                        error = (payload as? KickedPayload)?.reason?.ifBlank { "Jam session ended or you were removed" } ?: "Jam session ended or you were removed",
                    )
            }

            MessageTypes.BUFFER_WAIT -> {
                val p = payload as? BufferWaitPayload ?: return
                _state.update { s ->
                    s.copy(
                        waitingFor = p.waitingFor,
                        members = s.members.map { it.copy(isBuffering = it.userId in p.waitingFor) },
                    )
                }
            }

            MessageTypes.BUFFER_COMPLETE ->
                _state.update { s -> s.copy(waitingFor = emptyList(), members = s.members.map { it.copy(isBuffering = false) }) }

            MessageTypes.SYNC_STATE -> {
                val p = payload as? SyncStatePayload ?: return
                _state.update {
                    it.copy(
                        currentTrack = p.currentTrack,
                        isPlaying = p.isPlaying,
                        position = p.position,
                        // Only replace the queue when the server actually sent one — sync_state
                        // with an empty queue means "nothing to say", not "the queue is empty".
                        queue = p.queue.ifEmpty { it.queue },
                    )
                }
            }

            MessageTypes.SYNC_PLAYBACK, MessageTypes.PLAYBACK_ACTION -> {
                val p = payload as? PlaybackActionPayload ?: return
                val parsedPerms = parseJamPermissions(p.queueTitle)
                _state.update {
                    val newCurrentTrack =
                        when (p.action) {
                            PlaybackActions.CHANGE_TRACK -> p.trackInfo ?: it.currentTrack
                            PlaybackActions.PLAY -> p.trackInfo ?: it.currentTrack
                            else -> it.currentTrack
                        }
                    val isQueueAction =
                        p.action == PlaybackActions.QUEUE_ADD ||
                            p.action == PlaybackActions.QUEUE_REMOVE ||
                            p.action == PlaybackActions.QUEUE_CLEAR ||
                            p.action == PlaybackActions.SYNC_QUEUE
                    it.copy(
                        currentTrack = newCurrentTrack,
                        // CHANGE_TRACK is always a paused intermediate state on the server. The
                        // following PLAY/PAUSE frame restores the captured intent. Keeping the old
                        // value here collapses the later PLAY=true into no StateFlow change, so a
                        // newly buffered track never receives the command that should start it.
                        isPlaying =
                            when (p.action) {
                                PlaybackActions.PAUSE -> false
                                PlaybackActions.PLAY -> true
                                PlaybackActions.CHANGE_TRACK -> false
                                else -> it.isPlaying
                            },
                        position = if (isQueueAction) it.position else p.position,
                        queue = if (isQueueAction && p.queue.isNotEmpty()) p.queue else (if (p.action == PlaybackActions.QUEUE_CLEAR) emptyList() else p.queue.ifEmpty { it.queue }),
                        lastActionServerTime = if (isQueueAction) it.lastActionServerTime else p.capturedAtServerTime,
                        permissions = parsedPerms ?: it.permissions,
                    )
                }
            }

            MessageTypes.SUGGESTION_RECEIVED -> {
                val p = payload as? SuggestionReceivedPayload ?: return
                val track = p.trackInfo ?: return
                if (autoApproveSuggestions) {
                    approveSuggestion(p.suggestionId)
                    return
                }
                _state.update { s ->
                    if (s.suggestions.any { it.suggestionId == p.suggestionId }) {
                        s
                    } else {
                        s.copy(suggestions = s.suggestions + PendingSuggestion(p.suggestionId, p.fromUsername, track))
                    }
                }
            }

            MessageTypes.CHAT -> {
                val p = payload as? ChatMessagePayload ?: return
                if (p.reactions.isNotEmpty() && p.text.isBlank()) {
                    _state.update { s ->
                        s.copy(
                            chatMessages =
                                s.chatMessages.map { msg ->
                                    if (msg.id == p.id) msg.copy(reactions = p.reactions) else msg
                                },
                        )
                    }
                } else if (p.text.isNotBlank()) {
                    val (senderName, senderAvatar) = parseUserAndAvatar(p.senderName)
                    val chatMsg =
                        ChatMessageState(
                            id = p.id.ifBlank { "msg_${p.timestamp}_${p.senderId.take(4)}" },
                            senderId = p.senderId,
                            senderName = senderName,
                            senderAvatar = p.senderAvatar.ifBlank { senderAvatar },
                            text = p.text,
                            timestamp = p.timestamp,
                            replyToId = p.replyToId.ifBlank { null },
                            replyToText = p.replyToText.ifBlank { null },
                            replyToSenderName = p.replyToSenderName.ifBlank { null },
                            reactions = p.reactions,
                        )
                    _state.update { s ->
                        if (s.chatMessages.none { it.id == chatMsg.id }) {
                            s.copy(chatMessages = s.chatMessages + chatMsg)
                        } else {
                            s
                        }
                    }
                }
            }

            MessageTypes.ERROR -> {
                val p = payload as? ErrorPayload ?: return
                // `rate_limited` and `stale_track` are in-flight race conditions, not fatal session errors.
                if (p.code != "rate_limited" && p.code != "stale_track") {
                    Logger.w(TAG, "Server error ${p.code}: ${p.message}")
                    val isTerminal =
                        p.code in setOf(
                            "session_expired",
                            "session_not_found",
                            "room_not_found",
                            "room_closed",
                            "not_in_room",
                        )
                    if (isTerminal) {
                        client.clearSessionToken()
                        _state.update {
                            ListenTogetherState(
                                connection = it.connection,
                                error = p.message.ifBlank { "Jam session expired or ended" },
                            )
                        }
                    } else {
                        _state.update {
                            it.copy(pendingJoinCode = null, error = p.message.ifBlank { p.code })
                        }
                    }
                }
            }
        }
    }
}

fun parseUserAndAvatar(raw: String): Pair<String, String?> {
    if (raw.contains("\u001f")) {
        val parts = raw.split("\u001f", limit = 2)
        return parts[0].trim() to parts.getOrNull(1)?.trim()?.ifBlank { null }
    }
    if (raw.contains("|||")) {
        val parts = raw.split("|||", limit = 2)
        return parts[0].trim() to parts.getOrNull(1)?.trim()?.ifBlank { null }
    }
    val httpIdx = raw.indexOf("http://").takeIf { it >= 0 } ?: raw.indexOf("https://").takeIf { it >= 0 }
    if (httpIdx != null && httpIdx > 0) {
        val name = raw.substring(0, httpIdx).trim()
        val url = raw.substring(httpIdx).trim()
        return (if (name.isBlank()) "User" else name) to url.ifBlank { null }
    }
    return raw.trim() to null
}

fun formatJamPermissions(p: JamPermissionsState): String =
    "JAM_PERMS:q=${if (p.allowQueue) 1 else 0},r=${if (p.allowReorder) 1 else 0},d=${if (p.allowPlayDirect) 1 else 0},s=${if (p.allowSeek) 1 else 0},p=${if (p.allowPlayPause) 1 else 0}"

fun parseJamPermissions(header: String?): JamPermissionsState? {
    if (header == null || !header.startsWith("JAM_PERMS:")) return null
    val pairs = header.removePrefix("JAM_PERMS:").split(",")
    var q = true
    var r = false
    var d = false
    var s = false
    var p = false
    for (pair in pairs) {
        val kv = pair.split("=")
        if (kv.size == 2) {
            val v = kv[1] == "1"
            when (kv[0]) {
                "q" -> q = v
                "r" -> r = v
                "d" -> d = v
                "s" -> s = v
                "p" -> p = v
            }
        }
    }
    return JamPermissionsState(
        allowQueue = q,
        allowReorder = r,
        allowPlayDirect = d,
        allowSeek = s,
        allowPlayPause = p,
    )
}

private fun UserInfo.toMember(): RoomMember {
    val (name, avatar) = parseUserAndAvatar(username)
    return RoomMember(
        userId = userId,
        username = name,
        isHost = isHost,
        isConnected = isConnected,
        avatarUrl = avatar,
    )
}
