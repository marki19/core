package com.maxrave.domain.data.model.listentogether

/**
 * Listen Together, as the rest of the app sees it.
 *
 * These mirror the protocol types in `core/service/listenTogether` on purpose. The service module
 * is the wire format — its shapes are dictated by Metrolist's `.proto` and may not be changed —
 * while these belong to this app and can. Repeating them is what keeps the UI from importing the
 * service directly and pinning our screens to someone else's schema.
 */

/** Where the socket is, independent of whether a room has been joined. */
sealed interface RoomConnection {
    data object Disconnected : RoomConnection

    data object Connecting : RoomConnection

    data class Connected(val serverVersion: String) : RoomConnection

    /** Retries are exhausted or the server refused us; the user has to act. */
    data class Failed(val reason: String) : RoomConnection
}

data class RoomMember(
    val userId: String,
    val username: String,
    val isHost: Boolean,
    val isConnected: Boolean,
    val avatarUrl: String? = null,
    /** True while this member has not reported the current track as ready. */
    val isBuffering: Boolean = false,
)

data class RoomJoinRequest(
    val userId: String,
    val username: String,
    val avatarUrl: String? = null,
)

data class RoomTrack(
    val id: String,
    val title: String,
    val artist: String,
    val album: String = "",
    val durationMs: Long = 0L,
    val thumbnail: String = "",
)

data class RoomSuggestion(
    val suggestionId: String,
    val fromUsername: String,
    val track: RoomTrack,
)

data class JamPermissions(
    val allowQueue: Boolean = true,
    val allowReorder: Boolean = false,
    val allowPlayDirect: Boolean = false,
    val allowSeek: Boolean = false,
    val allowPlayPause: Boolean = false,
)

data class JamChatMessage(
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

data class ListenTogetherRoom(
    val connection: RoomConnection = RoomConnection.Disconnected,
    val roomCode: String? = null,
    val selfUserId: String = "",
    val isHost: Boolean = false,
    val members: List<RoomMember> = emptyList(),
    val joinRequests: List<RoomJoinRequest> = emptyList(),
    val suggestions: List<RoomSuggestion> = emptyList(),
    val currentTrack: RoomTrack? = null,
    val queue: List<RoomTrack> = emptyList(),
    val isPlaying: Boolean = false,
    val position: Long = 0L,
    val permissions: JamPermissions = JamPermissions(),
    val chatMessages: List<JamChatMessage> = emptyList(),
    /** Non-empty while the room is held at the buffer barrier. */
    val waitingFor: List<String> = emptyList(),
    /** The code we asked to join and have not heard back about. */
    val pendingJoinCode: String? = null,
    val error: String? = null,
) {
    val inRoom: Boolean get() = roomCode != null
    val isConnected: Boolean get() = connection is RoomConnection.Connected

    /** Names, not ids — "Waiting for Long" is the only useful phrasing of the barrier. */
    @Suppress("unused")
    val waitingForNames: List<String>
        get() = waitingFor.mapNotNull { id -> members.firstOrNull { it.userId == id }?.username }
}
