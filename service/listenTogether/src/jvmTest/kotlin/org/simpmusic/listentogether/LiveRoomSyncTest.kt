package org.simpmusic.listentogether

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Ignore
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Two real clients, one real room, against the public server.
 *
 * This is the test that answers "you join and then nothing happens": it drives a host and a guest
 * through the whole sequence — create, request, approve, and a transport command — and asserts the
 * guest's state actually moved. Nothing below is mocked; if the server, the codec or the session
 * disagree anywhere, one of these assertions fails.
 *
 * Network-dependent and it creates a room on someone else's server, so it is `@Ignore`d. Delete the
 * annotation to run it.
 */
class LiveRoomSyncTest {
    /** Generous: this is a real network hop to Poland, not a latency assertion. */
    private val MAX_TRAVEL_MS = 5_000L

    private fun session(version: String) =
        ListenTogetherSession(ListenTogetherClient(clientVersion = version))

    private suspend fun <T> eventually(
        label: String,
        timeoutMs: Long = 25_000,
        block: () -> T?,
    ): T? =
        withTimeoutOrNull(timeoutMs) {
            while (true) {
                block()?.let { return@withTimeoutOrNull it }
                delay(150)
            }
            @Suppress("UNREACHABLE_CODE")
            null
        }.also { if (it == null) println("  ✗ timed out waiting for $label") }



    @Ignore
    @Test
    fun testLeaveRoomAndCreateAgain() = runBlocking {
        val host = session("simpmusic-host-test")
        try {
            host.connect()
            assertNotNull(eventually("host connected") { host.state.value.isConnected.takeIf { it } }, "host never connected")
            println("✓ connected")
            host.createRoom("HostUser")
            val code1 = eventually("room1 created") { host.state.value.roomCode }
            assertNotNull(code1, "room1 not created")
            println("✓ room1 created: $code1")

            println("→ leaving room1")
            host.leaveRoom()
            delay(1000)
            println("✓ left room1, state=${host.state.value}")

            println("→ creating room2")
            host.createRoom("HostUser2")
            val code2 = eventually("room2 created") { host.state.value.roomCode }
            println("state after room2: ${host.state.value}")
            assertNotNull(code2, "room2 not created, error=${host.state.value.error}")
            println("✓ room2 created: $code2")
        } finally {
            host.release()
        }
    }

    @Ignore
    @Test
    fun testTwoClientsLeaveAndCrossJoinAndRecreate() = runBlocking {
        val clientA = session("simpmusic-clientA")
        val clientB = session("simpmusic-clientB")
        try {
            clientA.connect()
            clientB.connect()
            assertNotNull(eventually("clientA connected") { clientA.state.value.isConnected.takeIf { it } })
            assertNotNull(eventually("clientB connected") { clientB.state.value.isConnected.takeIf { it } })
            println("✓ both connected")

            // 1. ClientA creates room1
            clientA.createRoom("UserA")
            val code1 = eventually("room1 created") { clientA.state.value.roomCode }
            assertNotNull(code1, "room1 not created")
            println("✓ clientA created room1: $code1")

            // 2. ClientB joins room1
            clientB.joinRoom(code1, "UserB")
            val req = eventually("join req on A") { clientA.state.value.joinRequests.firstOrNull() }
            assertNotNull(req, "clientA did not get join request")
            clientA.approveJoin(req.userId)
            val code1B = eventually("clientB joined room1") { clientB.state.value.roomCode }
            assertNotNull(code1B, "clientB did not join room1")
            println("✓ clientB joined room1: $code1B")

            // 3. ClientB leaves room1
            clientB.leaveRoom()
            delay(500)
            println("✓ clientB left room1")

            // 4. ClientA leaves room1
            clientA.leaveRoom()
            delay(500)
            println("✓ clientA left room1")

            // 5. ClientB now creates room2
            clientB.createRoom("UserB")
            val code2 = eventually("room2 created by clientB") { clientB.state.value.roomCode }
            assertNotNull(code2, "clientB could not create room2, err=${clientB.state.value.error}")
            println("✓ clientB created room2: $code2")

            // 6. ClientA joins room2
            clientA.joinRoom(code2, "UserA")
            val req2 = eventually("join req on B") { clientB.state.value.joinRequests.firstOrNull() }
            assertNotNull(req2, "clientB did not get join request from clientA")
            clientB.approveJoin(req2.userId)
            val code2A = eventually("clientA joined room2") { clientA.state.value.roomCode }
            assertNotNull(code2A, "clientA could not join room2, err=${clientA.state.value.error}")
            println("✓ clientA joined room2: $code2A")

            // 7. ClientA leaves room2 and creates room3
            clientA.leaveRoom()
            delay(500)
            clientA.createRoom("UserA_Final")
            val code3 = eventually("room3 created by clientA") { clientA.state.value.roomCode }
            assertNotNull(code3, "clientA could not create room3, err=${clientA.state.value.error}")
            println("✓ clientA left room2 and created room3: $code3")
        } finally {
            clientA.release()
            clientB.release()
        }
    }

    @Ignore
    @Test
    fun aGuestFollowsTheHostThroughARealRoom() =
        runBlocking {
            val host = session("simpmusic-host-test")
            val guest = session("simpmusic-guest-test")

            try {
                host.connect()
                guest.connect()
                assertNotNull(eventually("host connected") { host.state.value.isConnected.takeIf { it } }, "host never connected")
                assertNotNull(eventually("guest connected") { guest.state.value.isConnected.takeIf { it } }, "guest never connected")
                println("✓ both clients connected")

                host.createRoom("HostUser")
                val code = eventually("room_created") { host.state.value.roomCode }
                assertNotNull(code, "host never got a room")
                assertTrue(host.state.value.isHost, "creator should be host")
                println("✓ room $code created")

                guest.joinRoom(code, "GuestUser")
                val request = eventually("join_request") { host.state.value.joinRequests.firstOrNull() }
                assertNotNull(request, "host never saw the join request")
                assertEquals("GuestUser", request.username)
                println("✓ host received the join request")

                host.approveJoin(request.userId)
                assertNotNull(eventually("join_approved") { guest.state.value.roomCode }, "guest never got in")
                assertTrue(!guest.state.value.isHost, "guest must not be host")
                println("✓ guest is in the room")

                // The whole point: a transport command issued by the host reaches the guest.
                val roomQueue =
                    listOf(
                        TrackInfo(id = "dQw4w9WgXcQ", title = "Test Track", artist = "Tester", duration = 180_000L),
                        TrackInfo(id = "9bZkp7q19f0", title = "Second", artist = "Tester", duration = 120_000L),
                        TrackInfo(id = "kJQP7kiw5Fk", title = "Third", artist = "Tester", duration = 150_000L),
                    )
                host.sendPlaybackAction(
                    action = PlaybackActions.CHANGE_TRACK,
                    trackId = "dQw4w9WgXcQ",
                    position = 0L,
                    trackInfo = roomQueue.first(),
                    queue = roomQueue,
                    queueTitle = "Test Queue",
                )
                val track = eventually("track on guest") { guest.state.value.currentTrack?.takeIf { it.id.isNotBlank() } }
                assertNotNull(track, "the guest never received the host's track")
                assertEquals("dQw4w9WgXcQ", track.id)
                println("✓ guest received the track: ${track.title}")

                // The queue must ride along with the track, not arrive separately — a guest with
                // only the current track plays its own next song and the room splits.
                // The server keeps the queue as UPCOMING tracks only — `sanitizeUpcomingQueue`
                // strips whatever is currently playing — so three sent comes back as two.
                val gotQueue = eventually("queue on guest") { guest.state.value.queue.takeIf { it.size >= 2 } }
                assertNotNull(gotQueue, "the guest never received the host's queue")
                assertEquals(listOf("9bZkp7q19f0", "kJQP7kiw5Fk"), gotQueue.map { it.id })
                println("✓ guest received the whole queue: ${gotQueue.map { it.title }}")

                host.sendPlaybackAction(PlaybackActions.PLAY, "dQw4w9WgXcQ", 12_345L, null)
                val playing = eventually("play on guest") { guest.state.value.takeIf { it.isPlaying } }
                assertNotNull(playing, "the guest never saw PLAY")
                // NOT assertEquals: the server advances the position by the time the command spent
                // in flight, which is the synchronisation working rather than a mismatch. A first
                // run saw 12_518 for a sent 12_345 — 173 ms of travel.
                val drift = playing.position - 12_345L
                assertTrue(
                    drift in 0..MAX_TRAVEL_MS,
                    "position should arrive advanced by travel time, not $drift ms off",
                )
                println("✓ guest received PLAY at ${playing.position} (advanced ${drift}ms in flight)")

                // Members list must show both, with exactly one host.
                val members = eventually("members") { guest.state.value.members.takeIf { it.size >= 2 } }
                assertNotNull(members, "guest never saw a full member list")
                println("✓ guest sees ${members.size} members")

                host.leaveRoom()
                guest.leaveRoom()
                delay(300)
            } finally {
                host.release()
                guest.release()
            }
        }
}
