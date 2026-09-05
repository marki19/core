package com.maxrave.data.listentogether

import com.maxrave.common.MERGING_DATA_TYPE
import com.maxrave.domain.data.model.browse.album.Track
import com.maxrave.domain.data.model.listentogether.RoomTrack
import com.maxrave.domain.data.player.GenericMediaItem
import com.maxrave.domain.data.player.GenericMediaMetadata
import com.maxrave.domain.mediaservice.handler.MediaPlayerHandler
import com.maxrave.domain.mediaservice.handler.SimpleMediaState
import com.maxrave.domain.repository.ListenTogetherRepository
import com.maxrave.logger.Logger
import kotlin.math.abs
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.simpmusic.listentogether.ListenTogetherSession
import org.simpmusic.listentogether.PlaybackActions
import org.simpmusic.listentogether.TrackInfo

private const val TAG = "ListenTogetherBridge"

/** What the guest reacts to. A data class so `distinctUntilChanged` compares every field. */
private data class RoomSnapshot(
    val track: RoomTrack?,
    val isPlaying: Boolean,
    val position: Long,
    val queueIds: List<String>,
)

/**
 * Joins a Listen Together room to the local player.
 *
 * Lives in `commonMain` and talks only to [MediaPlayerHandler], which is the one interface both
 * platforms implement — Android's `MediaServiceHandlerImpl` and Desktop's
 * `JvmMediaPlayerHandlerImpl` share nothing else, so anything written against a concrete player
 * would have to be written twice and would drift.
 *
 * Direction of travel is decided entirely by who hosts:
 * - **Host** watches the local player and publishes what it does.
 * - **Guest** watches the room and applies what the host did, and publishes nothing.
 *
 * The [applyingRemote] guard is what stops those two from feeding each other: applying a remote
 * pause makes the local player report "paused", which would otherwise be published straight back
 * to the server as a fresh command.
 */
class ListenTogetherPlaybackBridge(
    /**
     * Read through the repository (domain types), written through the session (protocol types).
     *
     * The split is deliberate: reacting to a room is app logic and belongs on our own model, while
     * publishing a command is protocol detail that must not leak upward — putting
     * `sendPlaybackAction` on the domain interface would drag Metrolist's schema into domain.
     */
    private val repository: ListenTogetherRepository,
    private val session: ListenTogetherSession,
    private val handler: MediaPlayerHandler,
    private val scope: CoroutineScope,
) {
    private var started = false
    private var applyingRemote = false
    private var lastPublishedTrackId: String? = null
    private var lastAppliedTrackId: String? = null
    private var lastAppliedQueueIds: List<String> = emptyList()
    private var lastPublishedIsPlaying: Boolean? = null

    /**
     * Whether the room was playing before the command currently being applied.
     *
     * Needed because change_track always says "not playing" — see [watchRoomPlayback].
     */
    private var lastRoomPlaying = false

    /**
     * Monotonic time (ms) when the current track was first applied. Drift correction is disabled
     * for the initial buffer window after a track change so a Member becoming READY ahead of the
     * host does not start independently and then get snapped back. Combined with the buffer
     * barrier this keeps every Member locked to the host's "play from 0:00" moment.
     */
    private var trackEpochMs: Long = 0L

    /** Idempotent: callers cannot know whether something else already started it. */
    fun start() {
        if (started) return
        started = true
        Logger.i(TAG, "Playback bridge started")
        scope.launch {
            launch { suppressCrossfadeWhileInRoom() }
            launch { watchRoomPlayback() }
            launch { resyncGuestOnResume() }
            launch { periodicDriftCorrection() }
            launch { pongDriftCorrection() }
            launch { publishCurrentStateOnJoin() }
            launch { publishQueueAsHost() }
            launch { publishTrackChangesAsHost() }
            launch { publishPlayPauseAsHost() }
            launch { publishSeeksAsHost() }
            launch { answerBufferBarrier() }
            launch { handleAutoAdvanceAsHost() }
        }
    }

    /**
     * Crossfade overlaps two tracks for seconds, which drifts a room apart at every transition.
     *
     * The user's own setting is left alone — this is a separate override on the player, the same
     * shape as the sleep-timer fade, so a process death mid-room cannot lose their preference.
     */
    private suspend fun suppressCrossfadeWhileInRoom() {
        repository.room
            .map { it.inRoom }
            .distinctUntilChanged()
            .collect { inRoom ->
                withContext(Dispatchers.Main) { handler.player.crossfadeSuppressed = inRoom }
                if (!inRoom) {
                    lastRoomPlaying = false
                } else if (!repository.room.value.isHost) {
                    // Ask for the live position the moment we are in. The state pushed on join
                    // carries the position as of the host's LAST command, which can be minutes old
                    // — obeying it drops a joiner at the start of a song everyone else is halfway
                    // through. sync_state answers with where the room actually is.
                    repository.requestSync()
                }
                Logger.i(TAG, if (inRoom) "Crossfade suppressed for the room" else "Crossfade restored")
            }
    }

    /**
     * A guest may pause, and stays paused.
     *
     * An earlier version forced the guest straight back to the room's state, which made pause
     * impossible — press it and playback resumed instantly. The room is something you listen along
     * with, not something that holds your transport hostage: pausing is local and silent, and
     * pressing play again asks the server where the room actually is now, so resuming lands in the
     * right place instead of wherever this device stopped. This is what Metrolist's manager does
     * (`requestSync` — "call this when a guest presses play/pause").
     */
    private suspend fun resyncGuestOnResume() {
        handler.controlState
            .map { it.isPlaying }
            .distinctUntilChanged()
            .collect { locallyPlaying ->
                val room = repository.room.value
                if (!room.inRoom || room.isHost || applyingRemote) return@collect
                if (!locallyPlaying) {
                    Logger.i(TAG, "Guest paused locally — leaving the room running")
                    return@collect
                }
                Logger.i(TAG, "Guest resumed — asking the server where the room is")
                repository.requestSync()
            }
    }

    // ─────────────────────────── guest: periodic drift correction ───────────────────────────

    /**
     * Background clock-skew corrector.
     *
     * Explicit room commands (play, pause, seek, change_track) land immediately and are already
     * clock-corrected by ServerClock. But long-running sessions accumulate hardware clock drift:
     * two devices whose clocks differ by 50 ppm will be 180 ms apart after one hour.
     *
     * Every [DRIFT_CHECK_INTERVAL_MS] this loop wakes up, computes where the room *should* be
     * right now using the server's authoritative `(position, lastActionServerTime)` tuple plus
     * the calibrated ServerClock, and silently seeks if the gap exceeds [SEEK_TOLERANCE_MS].
     *
     * The check is skipped when:
     * - The room is not in a playing state (paused / no track) — nothing to correct.
     * - The guest paused locally (`!localPlayWhenReady`) — respecting their local pause.
     * - We are currently applying a remote command (`applyingRemote`) — avoid a race.
     * - We are the host — the host IS the authoritative source, correcting against yourself
     *   makes no sense and could fight with explicit seek publishes.
     * - The current track epoch has not yet elapsed ([trackEpochMs]) — new-track initialization
     *   sets its own authoritative position; drift correction is disabled until that settles.
     */
    private suspend fun periodicDriftCorrection() {
        while (true) {
            delay(DRIFT_CHECK_INTERVAL_MS.milliseconds)
            val room = repository.room.value
            if (!room.inRoom || !room.isPlaying || room.currentTrack == null) continue
            if (room.isHost || applyingRemote) continue

            // Disable correction during the new-track epoch until it has elapsed.
            val nowMs = PROCESS_START.elapsedNow().inWholeMilliseconds
            if (nowMs - trackEpochMs < NEW_TRACK_EPOCH_MS) continue

            val authoritativePos = session.positionAt(room.position, room.isPlaying)
            val (localPos, localPlayWhenReady, isPlayingNow) = withContext(Dispatchers.Main) {
                Triple(handler.player.currentPosition, handler.player.playWhenReady, handler.player.isPlaying)
            }
            // Respect a local pause the guest made intentionally, or wait if still buffering/loading.
            if (!localPlayWhenReady || !isPlayingNow) continue

            val drift = abs(localPos - authoritativePos)
            if (drift > SEEK_TOLERANCE_MS) {
                Logger.i(
                    TAG,
                    "Periodic drift correction: local=$localPos auth=$authoritativePos drift=${drift}ms — seeking",
                )
                withContext(Dispatchers.Main) { applyDriftCorrection(authoritativePos) }
            } else {
                Logger.d(TAG, "Drift check: ${drift}ms — within tolerance, no correction needed")
            }
        }
    }

    /**
     * Instant drift correction from server-authoritative pong data.
     *
     * Complements [periodicDriftCorrection]: while the timer fires every [DRIFT_CHECK_INTERVAL_MS]
     * regardless of connection activity, this reacts the moment the server sends a pong carrying
     * an authoritative position. This is only possible once the Metroserver fork is updated to
     * populate those fields. On stock Metroserver, [session.authoritativePong] never emits. This
     * is a zero-overhead forward-compatible hook.
     *
     * Uses [applyDriftCorrection] so this path also participates in correction-stacking prevention.
     */
    private suspend fun pongDriftCorrection() {
        session.authoritativePong.collect { pong ->
            val room = repository.room.value
            if (!room.inRoom || !room.isHost || applyingRemote) return@collect
            if (!pong.authoritativeIsPlaying) return@collect
            // Verify the pong matches the track everyone is on — a stale pong during a track
            // transition should not seek the guest into the wrong position.
            if (room.currentTrack?.id != pong.authoritativeTrackId) return@collect

            // Disable correction during the new-track epoch.
            val nowMs = PROCESS_START.elapsedNow().inWholeMilliseconds
            if (nowMs - trackEpochMs < NEW_TRACK_EPOCH_MS) return@collect

            // Use the pong's own serverTime for a more precise correction than the last-action time.
            val authoritativePos = session.positionAt(
                pong.authoritativePosition,
                pong.authoritativeServerTime,
                pong.authoritativeIsPlaying,
            )
            val (localPos, localPlayWhenReady, isPlayingNow) = withContext(Dispatchers.Main) {
                Triple(handler.player.currentPosition, handler.player.playWhenReady, handler.player.isPlaying)
            }
            if (!localPlayWhenReady || !isPlayingNow) return@collect

            val drift = abs(localPos - authoritativePos)
            if (drift > SEEK_TOLERANCE_MS) {
                Logger.i(
                    TAG,
                    "Pong drift correction: local=$localPos auth=$authoritativePos drift=${drift}ms — seeking",
                )
                withContext(Dispatchers.Main) { applyDriftCorrection(authoritativePos) }
            }
        }
    }

    // ─────────────────────────── guest: follow the host ───────────────────────────

    private suspend fun watchRoomPlayback() {
        repository.room
            // The queue is part of the key: without it a room state that changed ONLY its queue
            // compares equal here and is dropped, so the guest never builds the host's queue at all.
            .map {
                RoomSnapshot(
                    track = it.currentTrack,
                    isPlaying = it.isPlaying,
                    position = it.position,
                    queueIds = it.queue.map { t -> t.id },
                ) to it.inRoom
            }
            .distinctUntilChanged()
            .collect { (snapshot, inRoom) ->
                if (!inRoom) return@collect
                val (track, isPlaying, position, queueIds) = snapshot
                val isHost = repository.room.value.isHost

                Logger.i(TAG, "Room says (${if (isHost) "host" else "guest"}): track=${track?.id} playing=$isPlaying pos=$position queue=${queueIds.size}")
                applyingRemote = true
                try {
                    val queueChanged = queueIds != lastAppliedQueueIds
                    val trackChanged = track != null && track.id.isNotBlank() && track.id != lastAppliedTrackId

                    val wasRoomPlaying = lastRoomPlaying
                    lastRoomPlaying = isPlaying

                    if (trackChanged && track != null) {
                        val isInitialJoin = lastAppliedTrackId == null
                        lastAppliedTrackId = track.id
                        lastPublishedTrackId = track.id
                        val shouldPlay = if (isInitialJoin) isPlaying else (isPlaying || wasRoomPlaying)
                        lastPublishedIsPlaying = shouldPlay
                        lastAppliedQueueIds = queueIds
                        // Mark the start of a new track epoch. Drift correction is disabled for
                        // [NEW_TRACK_EPOCH_MS] after this point so a Member that becomes READY
                        // before the host does not start independently and then get snapped back.
                        trackEpochMs = PROCESS_START.elapsedNow().inWholeMilliseconds
                        // If this is an ongoing track transition in the room, ALWAYS start at 0L (0:00).
                        // Only seek ahead if a guest is joining an already-playing room for the first time mid-song.
                        val startAt =
                            if (isInitialJoin && position > 0L) {
                                session.positionAt(position, isPlaying)
                            } else {
                                0L
                            }
                        val alreadyPlayingLocally = handler.nowPlaying.value?.mediaId == track.id
                        if (!alreadyPlayingLocally) {
                            // Pre-load the track in PAUSED state (playWhenReady = false) so both devices buffer silently at 0:00.
                            // Playback begins synchronously once the buffer barrier releases.
                            val initialPlay = if (isInitialJoin) shouldPlay else false
                            playTrack(track, keepPosition = startAt, playWhenReady = initialPlay)
                        } else if (!isInitialJoin) {
                            withContext(Dispatchers.Main) {
                                handler.player.seekTo(0L)
                                val canPlayNow = shouldPlay && repository.room.value.waitingFor.isEmpty()
                                if (canPlayNow) {
                                    handler.player.play()
                                } else {
                                    handler.player.pause()
                                }
                            }
                        }
                    } else if (queueChanged && track != null) {
                        lastAppliedQueueIds = queueIds
                        // Queue changed (reorder/add/remove): the room UI observes repository.room.value.queue.
                        // Do not touch active player timeline to prevent member restarts.
                    } else if (!isHost) {
                        // Apply play/pause transport or remote seek to guest.
                        applyTransport(isPlaying, position)
                    } else {
                        // On host: apply remote play/pause if a member with permission paused or
                        // resumed the room, and apply a remote seek if a member with seek permission
                        // dragged the scrubber. Without applying the seek here the host would keep
                        // playing from wherever it was and would only pick up the correction after the
                        // next pongDriftCorrection tick (≤10 s) — during which it can finish the
                        // current track and auto-advance before the backward seek ever lands.
                        //
                        // applyingRemote is already true at this point, so publishSeeksAsHost
                        // (which guards on applyingRemote) will not echo this seek back to the server.
                        withContext(Dispatchers.Main) {
                            // Seek first so the play/pause command lands at the right position.
                            val corrected = session.positionAt(position, isPlaying)
                            val isBuffering = handler.simpleMediaState.value is SimpleMediaState.Buffering
                            if (!isBuffering && abs(handler.player.currentPosition - corrected) > SEEK_TOLERANCE_MS) {
                                Logger.i(
                                    TAG,
                                    "Host applying remote seek to $corrected (room pos=$position, local=${handler.player.currentPosition})",
                                )
                                applyDriftCorrection(corrected)
                            }
                            if (isPlaying && !handler.player.playWhenReady) {
                                lastPublishedIsPlaying = true
                                if (abs(handler.player.currentPosition) < 1000L && position < 1000L) {
                                    handler.player.seekTo(0L)
                                }
                                handler.player.play()
                            } else if (!isPlaying && handler.player.playWhenReady) {
                                lastPublishedIsPlaying = false
                                handler.player.pause()
                            }
                        }
                    }
                } catch (e: Exception) {
                    Logger.e(TAG, "Failed to apply remote state: ${e.message}")
                } finally {
                    applyingRemote = false
                }
            }
    }

    private suspend fun applyTransport(
        isPlaying: Boolean,
        position: Long,
    ) = withContext(Dispatchers.Main) {
        // Correct the position for however long the command spent in flight; ServerClock falls back
        // to the raw value whenever it is not calibrated yet.
        val corrected = session.positionAt(position, isPlaying)
        // A small drift is normal and seeking on every tick would stutter; only a real gap is worth
        // a seek, which is also why the host publishes position with each command.
        if (abs(handler.player.currentPosition - corrected) > SEEK_TOLERANCE_MS) {
            applyDriftCorrection(corrected)
        }
        // playWhenReady, not isPlaying: a track that is still buffering reports isPlaying=false
        // while already committed to playing, so comparing against it re-issues play() every tick
        // and, worse, lets a stale pause land on a track that was about to start.
        // NOTE: the buffering check is intentionally omitted here. Both SEEK and PLAY must apply
        // immediately so their intent is not lost. ExoPlayer queues seeks correctly during buffering,
        // and calling play() while buffering sets playWhenReady=true so playback starts as soon as
        // the buffer fills — the previous check (which skipped play() while buffering) caused the
        // PLAY intent to be dropped on track transitions: the command arrived during buffer fill,
        // was skipped, and nothing ever retried it, leaving every new track permanently paused.
        if (isPlaying && !handler.player.playWhenReady) {
            if (abs(handler.player.currentPosition) < 1000L && corrected < 1000L) {
                handler.player.seekTo(0L)
            }
            handler.player.play()
        } else if (!isPlaying && handler.player.playWhenReady) {
            handler.player.pause()
        }
    }

    /**
     * Applies an authoritative position correction without delaying room-event collection.
     *
     * A seek itself is asynchronous in both players. Waiting for it here made the StateFlow
     * collector unavailable for up to two seconds; a CHANGE_TRACK then PLAY pair could conflate
     * into one "new track, playing" snapshot, after which the player loaded it paused and never
     * observed the PLAY. Issuing the seek is sufficient — subsequent state frames must be handled
     * immediately.
     */
    private suspend fun applyDriftCorrection(targetPositionMs: Long) {
        Logger.i(TAG, "Drift correction: seeking to $targetPositionMs")
        handler.player.seekTo(targetPositionMs)
    }

    /**
     * Loads the host's track locally.
     *
     * The room only carries a videoId, so the guest resolves its own metadata and stream — which is
     * exactly why two clients on different platforms, or a SimpMusic and a Metrolist client, can
     * share a room at all: both read the same catalogue rather than shipping audio to each other.
     */
    /**
     * Loads the host's track and queue.
     *
     * Built straight from the room's own [TrackInfo], the way Metrolist's manager does
     * (`TrackInfo.toMediaMetadata().toMediaItem()`) — deliberately NOT by resolving metadata from
     * the catalogue first. Two reasons, both learned the hard way:
     *
     * 1. `Track.toGenericMediaItem()` GUESSES song-vs-video from the artwork aspect ratio and
     *    treats the `maxresdefault.jpg` fallback as video. A guest resolving its own metadata lands
     *    on that branch almost every time, ends up on the merged audio+video path, and gets video
     *    with no sound where the host has plain audio.
     * 2. A network round trip per track can hang; `first { it.data != null }` on a flow that never
     *    carries data blocks the whole collector, and with it every later room command.
     *
     * The stream itself is still resolved locally by the player — the room only ever carries ids.
     */
    private suspend fun playTrack(
        info: RoomTrack,
        keepPosition: Long = 0L,
        playWhenReady: Boolean,
    ) {
        val roomQueue = repository.room.value.queue
        val ordered =
            (listOf(info) + roomQueue.filter { it.id != info.id })
                .filter { it.id.isNotBlank() }
                .distinctBy { it.id }
        Logger.i(TAG, "Guest loading ${info.id} (${info.title}) + ${ordered.size - 1} upcoming")

        // Dispatchers.Main is mandatory, not tidiness: Media3 throws if the player is touched off
        // the main thread, and the bridge runs on the service scope (Default).
        withContext(Dispatchers.Main) {
            handler.clearMediaItems()
            handler.addMediaItem(ordered.first().toRoomMediaItem(), playWhenReady = playWhenReady)
            if (keepPosition > 0L) {
                handler.player.seekTo(keepPosition)
            } else {
                handler.player.seekTo(0L)
            }
            if (playWhenReady) {
                handler.player.play()
            }
        }
    }

    /** Updates the upcoming tracks in the player's queue without interrupting current track playback. */
    private suspend fun updateQueueBehind(currentTrack: RoomTrack) {
        val roomQueue = repository.room.value.queue
        val ordered =
            (listOf(currentTrack) + roomQueue.filter { it.id != currentTrack.id })
                .filter { it.id.isNotBlank() }
                .distinctBy { it.id }
        val rest = ordered.drop(1)
        withContext(Dispatchers.Main) {
            try {
                val currentIndex = handler.player.currentMediaItemIndex
                while (handler.player.mediaItemCount > currentIndex + 1) {
                    handler.removeMediaItem(currentIndex + 1)
                }
                if (rest.isNotEmpty()) {
                    handler.addMediaItemList(rest.map { it.toRoomMediaItem() })
                }
            } catch (e: Exception) {
                Logger.e(TAG, "Error updating queue behind current track: ${e.message}")
            }
        }
    }

    /**
     * A room track as a media item.
     *
     * `MERGING_DATA_TYPE.SONG` is set explicitly: inside a room every client must be on the same
     * rendition, and for listening together that rendition is audio. Leaving it to be inferred is
     * what produced video-with-no-sound on the guest.
     */
    private fun RoomTrack.toRoomMediaItem(): GenericMediaItem =
        GenericMediaItem(
            mediaId = id,
            uri = id,
            metadata =
                GenericMediaMetadata(
                    title = title,
                    artist = artist.ifBlank { null },
                    albumTitle = album.ifBlank { null },
                    artworkUri = thumbnail.ifBlank { null },
                    description = MERGING_DATA_TYPE.SONG,
                ),
            customCacheKey = id,
        )

    // ─────────────────────────── host: publish what we do ───────────────────────────

    /**
     * Publishes what is ALREADY playing the moment we become host.
     *
     * Everything else here reacts to a *change* — a track transition, a play/pause. Someone who
     * was already listening and then opens a room produces neither, so without this the room has
     * no state at all and every guest sits in silence waiting for a command that only arrives if
     * the host happens to touch the transport.
     */
    private suspend fun publishCurrentStateOnJoin() {
        repository.room
            .map { it.inRoom && it.isHost }
            .distinctUntilChanged()
            .collect { isHosting -> if (isHosting) publishSnapshot() }
    }

    /** Republishes the queue whenever the host's own queue changes. */
    private suspend fun publishQueueAsHost() {
        handler.queueData
            .map { it?.data?.listTracks?.map { t -> t.videoId }.orEmpty() }
            .distinctUntilChanged()
            .collect { ids ->
                val state = repository.room.value
                if (!state.inRoom || !state.isHost || applyingRemote || ids.isEmpty()) return@collect
                publishQueue()
            }
    }

    private fun publishQueue() {
        val data = handler.queueData.value?.data ?: return
        val tracks = data.listTracks.map { it.toTrackInfo() }
        if (tracks.isEmpty()) return
        session.sendQueue(tracks, data.playlistName.orEmpty())
        Logger.i(TAG, "Published queue of ${tracks.size} track(s)")
    }

    private suspend fun publishSnapshot() {
        val item = handler.nowPlaying.value ?: return
        if (item.mediaId.isBlank()) return
        lastPublishedTrackId = item.mediaId
        lastAppliedTrackId = item.mediaId
        val data = handler.queueData.value?.data
        val (position, playWhenReady) =
            withContext(Dispatchers.Main) {
                handler.player.currentPosition to handler.player.playWhenReady
            }
        session.sendTrackChange(
            trackId = item.mediaId,
            position = position,
            trackInfo = item.toTrackInfo(),
            queue = data?.listTracks.orEmpty().map { it.toTrackInfo() },
            queueTitle = data?.playlistName.orEmpty(),
            shouldPlay = playWhenReady,
        )
        Logger.i(TAG, "Published current state to the room: ${item.mediaId}")
    }

    private suspend fun publishTrackChangesAsHost() {
        handler.nowPlaying
            .filterNotNull()
            .distinctUntilChanged { old, new -> old.mediaId == new.mediaId }
            .collect { item ->
                val state = repository.room.value
                val canControl = state.isHost || state.permissions.allowPlayDirect
                if (!state.inRoom || !canControl || applyingRemote) return@collect
                if (item.mediaId == lastPublishedTrackId) return@collect

                // Guard against the host navigating backward into local ExoPlayer history.
                //
                // The room queue contains only UPCOMING tracks; there is no concept of "previous"
                // in the room protocol. When the host presses Previous at ≤ 3 s, the local
                // ExoPlayer jumps to the previous media item in its personal timeline — a track
                // that guests and the server know nothing about.
                //
                // If the new track is neither the current room track nor anywhere in the upcoming
                // room queue, treat it as an out-of-room backwards navigation:
                //   1. Restore the player to the current room track at 0:00 (locally).
                //   2. Publish SEEK(0) so everyone restarts the current song in sync.
                // This preserves the "restart current song" semantic of Previous while keeping
                // the room in a coherent state.
                val roomState = repository.room.value
                val currentRoomTrackId = roomState.currentTrack?.id
                val isInRoomQueue = roomState.queue.any { it.id == item.mediaId }
                val isCurrentRoomTrack = item.mediaId == currentRoomTrackId

                if (!isCurrentRoomTrack && !isInRoomQueue && currentRoomTrackId != null) {
                    Logger.i(
                        TAG,
                        "Host navigated to out-of-room track ${item.mediaId} (not in queue, " +
                            "current room track=$currentRoomTrackId) — restoring room track and publishing SEEK(0)",
                    )
                    // Re-load the current room track. applyingRemote is NOT set here — we are the
                    // host acting on our own, so the restore must go through normally. We set
                    // lastPublishedTrackId/lastAppliedTrackId back to the room track so that when
                    // playTrack fires nowPlaying again it is recognised as no-change.
                    val roomTrack = roomState.currentTrack
                    if (roomTrack != null) {
                        lastPublishedTrackId = roomTrack.id
                        lastAppliedTrackId = roomTrack.id
                        // Read playWhenReady on Main — the player must not be touched off that thread.
                        val shouldPlay = withContext(Dispatchers.Main) { handler.player.playWhenReady }
                        playTrack(roomTrack, keepPosition = 0L, playWhenReady = shouldPlay)
                    }
                    // Tell the room to restart the current song from 0.
                    session.sendPlaybackAction(
                        action = PlaybackActions.SEEK,
                        trackId = "",
                        position = 0L,
                        trackInfo = null,
                    )
                    return@collect
                }

                lastPublishedTrackId = item.mediaId
                lastAppliedTrackId = item.mediaId
                // The now-playing item can change while it is buffering; playWhenReady is the
                // transport intent, whereas isPlaying is false during that normal loading gap.
                val shouldPlay = withContext(Dispatchers.Main) { handler.player.playWhenReady }
                Logger.i(TAG, "Host publishing track change: ${item.mediaId}")
                val remainingQueue = repository.room.value.queue.filterNot { it.id == item.mediaId }
                session.sendTrackChange(
                    trackId = item.mediaId,
                    position = 0L,
                    trackInfo = item.toTrackInfo(),
                    queue = remainingQueue.map { it.toTrackInfo() },
                    queueTitle = "",
                    shouldPlay = shouldPlay,
                )
                lastPublishedIsPlaying = shouldPlay
            }
    }

    private suspend fun publishPlayPauseAsHost() {
        handler.controlState
            .map { it.isPlaying }
            .distinctUntilChanged()
            .collect { isPlaying ->
                val state = repository.room.value
                val canControl = state.isHost || state.permissions.allowPlayPause
                if (!state.inRoom || !canControl || applyingRemote) return@collect
                if (state.waitingFor.isNotEmpty()) return@collect
                // A host that merely buffers reports isPlaying=false, indistinguishable from a
                // user pause — and publishing it stops the WHOLE room on one device's hiccup.
                // playWhenReady carries the intent, so a dip where the two disagree is not news.
                val (intent, currentPos) =
                    withContext(Dispatchers.Main) {
                        handler.player.playWhenReady to handler.player.currentPosition
                    }
                if (isPlaying != intent) return@collect
                if (lastPublishedIsPlaying == intent) return@collect
                lastPublishedIsPlaying = intent
                Logger.i(TAG, "Host publishing ${if (intent) "PLAY" else "PAUSE"}")
                session.sendPlaybackAction(
                    action = if (intent) PlaybackActions.PLAY else PlaybackActions.PAUSE,
                    // Deliberately EMPTY. The server rejects a play/pause whose trackId does not
                    // match the track it is holding ("stale_track") and drops it silently; sending
                    // nothing makes it fill in its own current track, which is always right.
                    trackId = "",
                    position = currentPos,
                    trackInfo = null,
                )
            }
    }

    /**
     * Publishes a seek.
     *
     * Neither `nowPlaying` nor `controlState` changes when the host drags the scrubber, so without
     * this a seek is simply never sent and guests keep playing from wherever they were.
     *
     * Detected off `SimpleMediaState.Progress`, which both platforms already emit — Metrolist uses
     * Media3's `onPositionDiscontinuity(DISCONTINUITY_REASON_SEEK)`, but that is an Android-only
     * API and Desktop runs mpv. A seek is a position that moved further than wall-clock time could
     * account for; ordinary playback advances roughly in step with it.
     */
    private suspend fun publishSeeksAsHost() {
        var lastProgress = 0L
        var lastAt = 0L
        handler.simpleMediaState.collect { mediaState ->
            val progress = (mediaState as? SimpleMediaState.Progress)?.progress ?: return@collect
            val now = PROCESS_START.elapsedNow().inWholeMilliseconds
            val previous = lastProgress
            val previousAt = lastAt
            lastProgress = progress
            lastAt = now

            val state = repository.room.value
            val canControl = state.isHost || state.permissions.allowSeek
            if (!state.inRoom || !canControl || applyingRemote) return@collect
            if (previousAt == 0L) return@collect

            val isPlaying = withContext(Dispatchers.Main) { handler.player.isPlaying }
            val elapsed = now - previousAt
            val expected = previous + if (isPlaying) elapsed else 0L
            if (abs(progress - expected) < SEEK_DETECT_MS) return@collect

            Logger.i(TAG, "Host publishing SEEK to $progress (expected ~$expected)")
            session.sendPlaybackAction(
                action = PlaybackActions.SEEK,
                trackId = "",
                position = progress,
                trackInfo = null,
            )
        }
    }

    // ─────────────────────────── the buffer barrier ───────────────────────────

    /**
     * Answers `buffer_ready` once the local player has the track the room is waiting on.
     *
     * Nobody in the room hears anything until every member answers, so a client that never sends
     * this silently freezes playback for everyone — including the host.
     */
    private suspend fun answerBufferBarrier() {
        var lastAnsweredTrackId: String? = null
        combine(
            repository.room.map { it.waitingFor to it.currentTrack?.id }.distinctUntilChanged(),
            handler.simpleMediaState,
        ) { (waitingFor, trackId), mediaState ->
            val room = repository.room.value
            if (trackId.isNullOrBlank() || !room.inRoom) return@combine
            if (room.selfUserId !in waitingFor) {
                if (lastAnsweredTrackId == trackId) {
                    lastAnsweredTrackId = null
                }
                return@combine
            }
            if (lastAnsweredTrackId == trackId) return@combine

            // bufferedPercentage, not isPlaying: the barrier asks whether the track is loaded,
            // and playback is exactly what it is holding back.
            val isReady = handler.player.bufferedPercentage >= READY_BUFFER_PERCENT ||
                mediaState is SimpleMediaState.Ready ||
                handler.player.duration > 0L
            if (isReady) {
                lastAnsweredTrackId = trackId
                session.reportBufferReady(trackId)
            }
        }.collect()
    }

    private suspend fun handleAutoAdvanceAsHost() {
        handler.simpleMediaState
            .collect { mediaState ->
                if (mediaState is SimpleMediaState.Ended) {
                    val state = repository.room.value
                    if (state.inRoom && state.isHost) {
                        val currentQueue = state.queue
                        if (currentQueue.isNotEmpty()) {
                            val nextTrack = currentQueue.first()
                            repository.playQueuedTrack(0, nextTrack)
                            // playQueuedTrack now sends CHANGE_TRACK + PLAY/PAUSE itself,
                            // so the new track resumes in the same state as the previous one.
                        } else {
                            // Queue is empty. We should pause the room to indicate playback finished.
                            withContext(Dispatchers.Main) {
                                handler.player.pause()
                                handler.player.seekTo(0L)
                            }
                        }
                    }
                }
            }
    }

    private fun Track.toTrackInfo(): TrackInfo =
        TrackInfo(
            id = videoId,
            title = title,
            artist = artists?.joinToString(", ") { it.name }.orEmpty(),
            album = album?.name.orEmpty(),
            duration = (durationSeconds?.toLong() ?: 0L) * 1000L,
            thumbnail = thumbnails?.lastOrNull()?.url.orEmpty(),
        )

    private fun RoomTrack.toTrackInfo(): TrackInfo =
        TrackInfo(
            id = id,
            title = title,
            artist = artist,
            album = album,
            duration = durationMs,
            thumbnail = thumbnail,
        )

    /** The room carries a videoId plus display metadata; the guest resolves its own stream. */
    private fun GenericMediaItem.toTrackInfo(): TrackInfo =
        TrackInfo(
            id = mediaId,
            title = metadata.title.orEmpty(),
            artist = metadata.artist.orEmpty(),
            album = metadata.albumTitle.orEmpty(),
            duration = handler.player.duration.coerceAtLeast(0L),
            thumbnail = metadata.artworkUri.orEmpty(),
        )

    private companion object {
        /** Monotonic reference for telling a seek apart from ordinary playback advancing. */
        val PROCESS_START = TimeSource.Monotonic.markNow()

        /**
         * Metrolist's own hard-sync threshold (`HARD_SYNC_THRESHOLD_MS`). Below it a seek costs
         * more in stutter than it buys in sync; above it the room is audibly apart.
         */
        const val SEEK_TOLERANCE_MS = 750L

        /**
         * A jump larger than this is a seek rather than playback advancing. Comfortably above the
         * progress tick interval so ordinary drift never trips it.
         */
        const val SEEK_DETECT_MS = 2_500L
        const val READY_BUFFER_PERCENT = 5

        /**
         * How often the background drift-correction loop wakes to compare the local position
         * against the server-authoritative one. This sits between ping intervals:
         * frequent enough to catch hardware clock skew before it becomes audible (~1 second),
         * infrequent enough to avoid constant seeking on a healthy connection.
         *
         * Rationale: at 50 ppm skew two devices diverge at ~50 ms/min. Checking every 10 s means
         * the worst-case uncorrected drift before the next check is < 10 ms — well below the
         * 750 ms tolerance and imperceptible. The correction is only applied when drift exceeds
         * [SEEK_TOLERANCE_MS], so a passing 10 s tick with 5 ms drift is a no-op.
         */
        const val DRIFT_CHECK_INTERVAL_MS = 10_000L

        /**
         * After a new track is applied to the local player, drift correction is suppressed for
         * this many milliseconds so the initial buffer/READY window does not cause the Member to
         * chase the host's moving position. A Member that finishes buffering before the host will
         * be paused by the buffer barrier anyway, but on a single-member room or a flaky network
         * the barrier does not block, and without this guard the periodic drift loop would
         * immediately try to seek the Member from 0:00 to wherever the host is now.
         *
         * The duration is intentionally short — long enough to let a typical local buffer settle
         * (≤ 500 ms for an audio track) but short enough that legitimate drift correction resumes
         * quickly. 3 s is the budget for the worst case: a 1 MB stream taking 1-2 s to load, plus
         * 1 s of headroom.
         */
        const val NEW_TRACK_EPOCH_MS = 3_000L
    }
}
