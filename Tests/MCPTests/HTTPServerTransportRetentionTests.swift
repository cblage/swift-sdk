import Foundation
import Testing

@testable import MCP

// MARK: - Test Helpers

private func makeRetainingTransport(
    maxBytes: Int = 1 << 20,
    maxEvents: Int,
    incomingCapacity: Int? = nil
) -> StatefulHTTPServerTransport {
    StatefulHTTPServerTransport(
        validationPipeline: StandardValidationPipeline(validators: []),
        replayRetention: .init(maxBytes: maxBytes, maxEvents: maxEvents),
        incomingCapacity: incomingCapacity
    )
}

/// One SSE frame as a client parses it: its `id:` line and its `data:` line.
private struct Frame: Equatable {
    var id: String?
    var data = ""
}

/// The complete frames held by raw SSE chunks, whatever the chunk boundaries.
private func frames(in chunks: [Data]) -> [Frame] {
    let text = String(decoding: chunks.reduce(into: Data()) { $0.append($1) }, as: UTF8.self)
    return text.components(separatedBy: "\n\n").compactMap { block -> Frame? in
        guard !block.isEmpty else { return nil }
        var frame = Frame()
        for line in block.split(separator: "\n") {
            if line.hasPrefix("id: ") { frame.id = String(line.dropFirst(4)) }
            if line.hasPrefix("data: ") { frame.data = String(line.dropFirst(6)) }
        }
        return frame
    }
}

private func body(of response: HTTPResponse) -> String {
    String(decoding: response.bodyData ?? Data(), as: UTF8.self)
}

private actor Flag {
    var isSet = false
    func set() { isSet = true }
}

private struct Drained {
    var chunks: [Data] = []
    /// Whether the stream ended on its own — finished or thrown — before the timeout.
    var ended = false
    var error: (any Error)?
}

/// Reads a response's stream until it ends or `timeout` passes.
private func drain(_ response: HTTPResponse, timeout: Duration) async -> Drained {
    guard case .stream(let stream, _) = response else { return Drained() }
    let collector = ChunkCollector()
    let timedOut = Flag()
    let reader = Task { () -> (any Error)? in
        do {
            for try await chunk in stream { await collector.append(chunk) }
            return nil
        } catch {
            return error
        }
    }
    let timer = Task {
        try await Task.sleep(for: timeout)
        await timedOut.set()
        reader.cancel()
    }
    let error = await reader.value
    timer.cancel()
    return Drained(chunks: await collector.getChunks(), ended: !(await timedOut.isSet), error: error)
}

// MARK: - Retention Tests

@Suite("StatefulHTTPServerTransport Retention Tests")
struct StatefulHTTPServerTransportRetentionTests {

    @Test("The replay window evicts its oldest events and names an evicted cursor")
    func testRollingRetentionEvictsOldestAndReportsEvictedCursor() async throws {
        let transport = makeRetainingTransport(maxEvents: 3)
        let sessionID = try await initializeSession(transport: transport)

        // Three notifications with no stream connected: the window holds them and nothing of
        // the initialize, whose priming event was 1_1 and response 1_2.
        for n in 1...3 {
            try await transport.send(makeNotificationBody(method: "note-n\(n)"))
        }

        let evicted = await transport.handleRequest(
            makeGETRequest(sessionID: sessionID, lastEventID: "1_1"))
        #expect(evicted.statusCode == 400)
        #expect(body(of: evicted).contains("replay window"))

        let unknown = await transport.handleRequest(
            makeGETRequest(sessionID: sessionID, lastEventID: "never_9999"))
        #expect(unknown.statusCode == 400)
        #expect(!body(of: unknown).contains("replay window"))

        let resumed = await transport.handleRequest(
            makeGETRequest(sessionID: sessionID, lastEventID: "_GET_stream_3"))
        #expect(resumed.statusCode == 200)
        let replayed = frames(in: await drain(resumed, timeout: .milliseconds(300)).chunks)
        #expect(replayed.map(\.id) == ["_GET_stream_4", "_GET_stream_5", "_GET_stream_6"])
        #expect(replayed[0].data.contains("note-n2"))
        #expect(replayed[1].data.contains("note-n3"))
        #expect(replayed[2].data.isEmpty, "the resumed stream is primed again")

        // The session itself stands after the gap.
        let request = await transport.handleRequest(
            makeStatefulPOSTRequest(body: makeRequestBody(id: "after"), sessionID: sessionID))
        #expect(request.statusCode == 200)

        await transport.disconnect()
    }

    @Test("Byte retention counts each frame with its identifiers")
    func testByteRetentionEvictsByFrameSize() async throws {
        let notification = makeNotificationBody(method: "notifications/padding")
        let frameBytes =
            SSEEvent.message(data: notification, id: "_GET_stream_10").formatted().count
            + "_GET_stream_10".utf8.count + "_GET_stream".utf8.count
        // Room for two notifications and a half: the third evicts the first.
        let transport = makeRetainingTransport(maxBytes: frameBytes * 5 / 2, maxEvents: 100)
        let sessionID = try await initializeSession(transport: transport)

        for _ in 1...3 { try await transport.send(notification) }

        let evicted = await transport.handleRequest(
            makeGETRequest(sessionID: sessionID, lastEventID: "_GET_stream_3"))
        #expect(evicted.statusCode == 400)
        #expect(body(of: evicted).contains("replay window"))

        let resumed = await transport.handleRequest(
            makeGETRequest(sessionID: sessionID, lastEventID: "_GET_stream_4"))
        #expect(resumed.statusCode == 200)
        let replayed = frames(in: await drain(resumed, timeout: .milliseconds(300)).chunks)
        #expect(replayed.map(\.id) == ["_GET_stream_5", "_GET_stream_6"])

        await transport.disconnect()
    }

    @Test("Priming events count toward the event bound")
    func testPrimingEventsAreRetainedEvents() async throws {
        let transport = makeRetainingTransport(maxEvents: 2)
        let sessionID = try await initializeSession(transport: transport)

        // The initialize left its priming event 1_1 and its response 1_2; the GET's priming
        // event is the third, and the window of two lets the first go.
        let get = await transport.handleRequest(makeGETRequest(sessionID: sessionID))
        #expect(get.statusCode == 200)

        let evicted = await transport.handleRequest(
            makeGETRequest(sessionID: sessionID, lastEventID: "1_1"))
        #expect(evicted.statusCode == 400)
        #expect(body(of: evicted).contains("replay window"))

        await transport.disconnect()
    }

    @Test("Replaying a retained response finishes the resumed request stream")
    func testResumedCompletedRequestStreamEnds() async throws {
        let transport = makeRetainingTransport(maxEvents: 16)
        let sessionID = try await initializeSession(transport: transport)

        let response = await transport.handleRequest(
            makeStatefulPOSTRequest(body: makeRequestBody(id: "r1"), sessionID: sessionID))
        #expect(response.statusCode == 200)
        let original = Task { await drain(response, timeout: .seconds(2)) }
        try await transport.send(makeResponseBody(id: "r1"))
        let answered = await original.value
        #expect(answered.ended)
        #expect(frames(in: answered.chunks).map(\.id) == ["r1_3", "r1_4"])

        // Resuming from the priming event replays the response and ends, as the original did.
        let resumed = await transport.handleRequest(
            makeGETRequest(sessionID: sessionID, lastEventID: "r1_3"))
        #expect(resumed.statusCode == 200)
        let replay = await drain(resumed, timeout: .seconds(2))
        #expect(replay.ended)
        #expect(replay.error == nil)
        #expect(frames(in: replay.chunks).map(\.id) == ["r1_4"])

        // Resuming from the response itself has nothing to replay and ends at once.
        let settled = await transport.handleRequest(
            makeGETRequest(sessionID: sessionID, lastEventID: "r1_4"))
        let nothing = await drain(settled, timeout: .seconds(2))
        #expect(nothing.ended)
        #expect(nothing.chunks.isEmpty)

        // The request's HTTP context went with its response.
        let context = await transport.httpRequestContext(for: .string("r1"))
        #expect(context == nil)

        await transport.disconnect()
    }

    @Test("A standalone stream whose reader goes away frees its slot for the next connection")
    func testDisconnectedStandaloneStreamFreesItsSlot() async throws {
        let transport = makeRetainingTransport(maxEvents: 16)
        let sessionID = try await initializeSession(transport: transport)

        let first = await transport.handleRequest(makeGETRequest(sessionID: sessionID))
        #expect(first.statusCode == 200)
        let held = await transport.handleRequest(makeGETRequest(sessionID: sessionID))
        #expect(held.statusCode == 409)

        // The reader hangs up: its task ends, and the slot it held goes with it.
        _ = await drain(first, timeout: .milliseconds(100))
        try await Task.sleep(for: .milliseconds(100))
        let second = await transport.handleRequest(makeGETRequest(sessionID: sessionID))
        #expect(second.statusCode == 200)

        // A late end of the first connection never clears the second's slot: its
        // notifications still arrive.
        try await Task.sleep(for: .milliseconds(100))
        let reading = Task { await drain(second, timeout: .milliseconds(500)) }
        try await transport.send(makeNotificationBody(method: "note-still-here"))
        let received = frames(in: await reading.value.chunks)
        #expect(received.contains { $0.data.contains("note-still-here") })

        await transport.disconnect()
    }

    @Test("A reader that cannot keep up ends only its stream, which resumes from its last complete event")
    func testSlowReaderEndsOnlyItsStream() async throws {
        // Two retained events within 4 KiB: a delivery queue of four chunks, a window of two.
        let transport = makeRetainingTransport(maxBytes: 4096, maxEvents: 2)
        let sessionID = try await initializeSession(transport: transport)

        let get = await transport.handleRequest(makeGETRequest(sessionID: sessionID))
        #expect(get.statusCode == 200)

        // Nobody reads: the priming event and three notifications fill the queue, and the
        // fourth cannot enter. It stays in the window; the stream ends for its resume.
        for n in 1...4 {
            try await transport.send(makeNotificationBody(method: "note-n\(n)"))
        }
        let drained = await drain(get, timeout: .seconds(1))
        #expect(drained.ended)
        #expect(drained.error != nil)
        #expect(
            frames(in: drained.chunks).map(\.id)
                == ["_GET_stream_3", "_GET_stream_4", "_GET_stream_5", "_GET_stream_6"])

        // The session stands: the last complete event resumes, replaying the one that never
        // entered the queue, and the stream is primed again.
        let resumed = await transport.handleRequest(
            makeGETRequest(sessionID: sessionID, lastEventID: "_GET_stream_6"))
        #expect(resumed.statusCode == 200)
        let replayed = frames(in: await drain(resumed, timeout: .milliseconds(300)).chunks)
        #expect(replayed.map(\.id) == ["_GET_stream_7", "_GET_stream_8"])
        #expect(replayed[0].data.contains("note-n4"))

        await transport.disconnect()
    }

    @Test("A full incoming queue refuses the message and leaves nothing behind")
    func testIncomingCapacityRefusesWithoutProvisionalState() async throws {
        let transport = makeRetainingTransport(maxEvents: 16, incomingCapacity: 1)
        let sessionID = try await initializeSession(transport: transport)

        // The initialize's reader has returned; nothing drains the queue now.
        let admitted = await transport.handleRequest(
            makeStatefulPOSTRequest(body: makeRequestBody(id: "a"), sessionID: sessionID))
        #expect(admitted.statusCode == 200)
        let refused = await transport.handleRequest(
            makeStatefulPOSTRequest(body: makeRequestBody(id: "b"), sessionID: sessionID))
        #expect(refused.statusCode == 429)
        let notification = await transport.handleRequest(
            makeStatefulPOSTRequest(body: makeNotificationBody(), sessionID: sessionID))
        #expect(notification.statusCode == 429)

        let kept = await transport.httpRequestContext(for: .string("a"))
        let dropped = await transport.httpRequestContext(for: .string("b"))
        #expect(kept != nil)
        #expect(dropped == nil)

        // No event was issued for the refused request: its would-be cursor is unknown.
        let cursor = await transport.handleRequest(
            makeGETRequest(sessionID: sessionID, lastEventID: "b_4"))
        #expect(cursor.statusCode == 400)
        #expect(!body(of: cursor).contains("replay window"))

        await transport.disconnect()
    }

    @Test("A resume replaces the older connection of the same stream")
    func testResumeReplacesOlderConnection() async throws {
        let transport = makeRetainingTransport(maxEvents: 16)
        let sessionID = try await initializeSession(transport: transport)

        let first = await transport.handleRequest(makeGETRequest(sessionID: sessionID))
        #expect(first.statusCode == 200)
        let firstReader = Task { await drain(first, timeout: .seconds(2)) }
        try await Task.sleep(for: .milliseconds(50))

        let resumed = await transport.handleRequest(
            makeGETRequest(sessionID: sessionID, lastEventID: "_GET_stream_3"))
        #expect(resumed.statusCode == 200)

        // The older connection ends; the resumed one carries what follows.
        let firstDrained = await firstReader.value
        #expect(firstDrained.ended)
        let reading = Task { await drain(resumed, timeout: .milliseconds(500)) }
        try await transport.send(makeNotificationBody(method: "note-after-resume"))
        let received = frames(in: await reading.value.chunks)
        #expect(received.contains { $0.data.contains("note-after-resume") })

        await transport.disconnect()
    }
}
