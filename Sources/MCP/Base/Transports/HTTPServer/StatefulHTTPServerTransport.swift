import Foundation
import Logging

/// A stateful HTTP server transport that manages sessions and uses SSE for streaming responses.
///
/// This transport implements the MCP Streamable HTTP specification with full session management:
/// - Assigns a session ID during initialization (via `Mcp-Session-Id` header)
/// - POST requests receive SSE-streamed responses
/// - GET requests establish a standalone SSE stream for server-initiated messages
/// - DELETE requests terminate the session
/// - Built-in event store for resumability (reconnection with `Last-Event-ID`)
///
/// ## Bounded retention
///
/// With a ``ReplayRetention`` the event store is a rolling window: the oldest events leave as
/// new ones arrive, a resume from an event that has left the window is refused with a
/// replay-window error, and the session itself stays usable. Each connected SSE stream then
/// has a finite delivery queue sized to hold a full replay; a reader that cannot keep up ends
/// that stream alone and resumes from its last complete event. With an `incomingCapacity` the
/// queue of client messages awaiting the server is finite too, a message it cannot take
/// answered with a capacity error rather than accepted.
///
/// ## Usage
///
/// ```swift
/// let transport = StatefulHTTPServerTransport()  // Uses UUID by default
///
/// // Start the MCP server with this transport
/// try await server.start(transport: transport)
///
/// // In your HTTP framework handler:
/// let response = await transport.handleRequest(httpRequest)
/// // Convert response to your framework's response type and return it
/// ```
///
/// ## Framework Integration
///
/// This transport is framework-agnostic. You provide incoming requests as `HTTPRequest`
/// and receive `HTTPResponse` values to convert to your framework's native types.
/// For SSE responses, the `.stream` case provides an `AsyncThrowingStream<Data, Error>`
/// to pipe to the client.
public actor StatefulHTTPServerTransport: Transport, HTTPContextProviding, RequestSettlementObserving {
    public nonisolated let logger: Logger

    /// The bounds of the rolling replay window: retained bytes — each event's SSE frame plus its
    /// event and stream identifiers — and retained event records, priming events included.
    public struct ReplayRetention: Sendable {
        public let maxBytes: Int
        public let maxEvents: Int

        public init(maxBytes: Int, maxEvents: Int) {
            self.maxBytes = maxBytes
            self.maxEvents = maxEvents
        }
    }

    /// The largest piece of an SSE frame offered to a delivery queue at once, so a queue bounded
    /// in chunks is bounded in bytes.
    package static let deliveryChunkSize = 16 * 1024

    // MARK: - Dependencies

    private let sessionIDGenerator: any SessionIDGenerator
    private let validationPipeline: any HTTPRequestValidationPipeline
    private let retryInterval: Int?
    private let replayRetention: ReplayRetention?

    /// Chunks a connected stream's queue holds: a whole replay of the retained window — one
    /// chunk per event for its partial last chunk, the window's bytes in whole chunks — and
    /// the priming event that follows it. `nil` is unbounded, as with no retention.
    private let deliveryCapacity: Int?

    // MARK: - State

    private var sessionID: String?
    private var terminated = false
    private var started = false

    // MARK: - Incoming message stream (client → server)

    private let incomingStream: AsyncThrowingStream<Data, Swift.Error>
    private let incomingContinuation: AsyncThrowingStream<Data, Swift.Error>.Continuation

    // MARK: - SSE streams

    /// One connected SSE stream: its continuation and the generation naming this connection,
    /// so the end of an older connection under the same stream never clears a newer one.
    private struct StreamSlot {
        let continuation: AsyncThrowingStream<Data, Swift.Error>.Continuation
        let generation: UInt64
    }

    /// Maps request ID → the connected SSE stream of an active POST request.
    private var requestStreams: [String: StreamSlot] = [:]

    /// Maps request ID → originating HTTP request, surfaced to handlers via
    /// ``Server/currentHTTPContext``. Cleared when the response is routed,
    /// the request settles without one, or the session terminates.
    private var httpRequestContexts: [String: HTTPRequest] = [:]

    /// The standalone SSE stream for server-initiated messages.
    /// Only one GET stream is allowed per session.
    private var standaloneStream: StreamSlot?

    /// Internal identifier for the standalone GET stream in the event store.
    private let standaloneStreamID = "_GET_stream"

    private var streamGeneration: UInt64 = 0

    // MARK: - Event Store (Resumability)

    private struct StoredEvent {
        let streamID: String
        let eventID: String
        /// The SSE frame replayed to a resuming client; `nil` for a priming event, which is
        /// retained for its count and never replayed.
        let frame: Data?
        /// Whether this event is its request stream's response, after which a resumed stream
        /// ends as the original did.
        let terminal: Bool

        var retainedBytes: Int { (frame?.count ?? 0) + eventID.utf8.count + streamID.utf8.count }
    }

    private var storedEvents: [StoredEvent] = []
    private var storedBytes = 0
    /// Never reset: event IDs stay unique for the session's life, evictions included.
    private var eventCounter: Int = 0

    // MARK: - Init

    /// Creates a new stateful HTTP server transport.
    ///
    /// - Parameters:
    ///   - sessionIDGenerator: Generator for session IDs. The IDs MUST contain
    ///     only visible ASCII characters (0x21-0x7E) per the MCP specification.
    ///     Defaults to ``UUIDSessionIDGenerator``.
    ///   - validationPipeline: Custom validation pipeline. If `nil`, uses sensible defaults:
    ///     origin validation (localhost), Accept header (SSE required), Content-Type,
    ///     protocol version, and session validation.
    ///   - retryInterval: Retry interval in milliseconds for SSE priming events.
    ///     Controls how long clients wait before attempting to reconnect.
    ///   - replayRetention: The rolling replay window's bounds. If `nil`, every event is
    ///     retained until the session terminates and delivery queues are unbounded.
    ///   - incomingCapacity: The most client messages that may await the server at once.
    ///     If `nil`, the queue is unbounded.
    ///   - logger: Optional logger. If `nil`, a no-op logger is used.
    public init(
        sessionIDGenerator: any SessionIDGenerator = UUIDSessionIDGenerator(),
        validationPipeline: (any HTTPRequestValidationPipeline)? = nil,
        retryInterval: Int? = nil,
        replayRetention: ReplayRetention? = nil,
        incomingCapacity: Int? = nil,
        logger: Logger? = nil
    ) {
        self.sessionIDGenerator = sessionIDGenerator
        self.validationPipeline = validationPipeline ?? StandardValidationPipeline(validators: [
            OriginValidator.localhost(),
            AcceptHeaderValidator(mode: .sseRequired),
            ContentTypeValidator(),
            ProtocolVersionValidator(),
            SessionValidator(),
        ])
        self.retryInterval = retryInterval
        self.replayRetention = replayRetention
        self.deliveryCapacity = replayRetention.map {
            $0.maxEvents + $0.maxBytes / Self.deliveryChunkSize + 2
        }
        self.logger = logger ?? Logger(
            label: "mcp.transport.http.server.stateful",
            factory: { _ in SwiftLogNoOpLogHandler() }
        )

        let (stream, continuation) = AsyncThrowingStream<Data, Swift.Error>.makeStream(
            bufferingPolicy: incomingCapacity.map { .bufferingOldest($0) } ?? .unbounded
        )
        self.incomingStream = stream
        self.incomingContinuation = continuation
    }

    // MARK: - Transport Conformance

    public func connect() async throws {
        guard !started else {
            throw MCPError.internalError("Transport already started")
        }
        started = true
        logger.debug("Stateful HTTP server transport started")
    }

    public func disconnect() async {
        terminate()
    }

    /// Routes outgoing server messages to the appropriate client connection.
    ///
    /// - Responses are routed to the SSE stream matching the response's JSON-RPC ID.
    /// - Notifications and server-initiated requests are routed to the standalone GET stream.
    public func send(_ data: Data) async throws {
        guard !terminated else {
            throw MCPError.connectionClosed
        }

        guard let kind = JSONRPCMessageKind(data: data) else {
            logger.warning("Could not classify outgoing message for routing")
            return
        }

        switch kind {
        case .response(let id):
            routeResponse(data, requestID: id)
        case .notification, .request:
            routeServerInitiatedMessage(data)
        }
    }

    public func receive() -> AsyncThrowingStream<Data, Swift.Error> {
        incomingStream
    }

    // MARK: - HTTP Request Handler

    /// Handles an incoming HTTP request from the framework adapter.
    ///
    /// Routes by HTTP method:
    /// - **POST**: JSON-RPC messages (requests, notifications)
    /// - **GET**: Establish standalone SSE stream for server-initiated messages
    /// - **DELETE**: Terminate the session
    /// - Others: 405 Method Not Allowed
    public func handleRequest(_ request: HTTPRequest) async -> HTTPResponse {
        if terminated {
            return .error(
                statusCode: 404,
                .invalidRequest("Not Found: Session has been terminated"),
                sessionID: sessionID
            )
        }

        switch request.method.uppercased() {
        case "POST":
            return handlePost(request)
        case "GET":
            return handleGet(request)
        case "DELETE":
            return handleDelete(request)
        default:
            return .error(
                statusCode: 405,
                .invalidRequest("Method Not Allowed"),
                sessionID: sessionID,
                extraHeaders: [HTTPHeaderName.allow: "GET, POST, DELETE"]
            )
        }
    }

    // MARK: - POST Handler

    private func handlePost(_ request: HTTPRequest) -> HTTPResponse {
        // Parse body first so we can determine if it's an initialization request
        guard let body = request.body, !body.isEmpty else {
            return .error(
                statusCode: 400,
                .parseError("Empty request body"),
                sessionID: sessionID
            )
        }

        guard let messageKind = JSONRPCMessageKind(data: body) else {
            return .error(
                statusCode: 400,
                .parseError("Invalid JSON-RPC message"),
                sessionID: sessionID
            )
        }

        // Build validation context
        let context = HTTPValidationContext(
            httpMethod: "POST",
            sessionID: sessionID,
            isInitializationRequest: messageKind.isInitializeRequest,
            supportedProtocolVersions: Version.supported
        )

        // Run validation pipeline
        if let errorResponse = validationPipeline.validate(request, context: context) {
            return errorResponse
        }

        // Handle initialization request specially
        if messageKind.isInitializeRequest {
            return handleInitializationRequest(body, request: request)
        }

        // Handle by message type
        switch messageKind {
        case .notification, .response:
            // Yield to server and return 202 Accepted
            guard enqueueIncoming(body) else { return incomingCapacityResponse() }
            return .accepted(headers: sessionHeaders())

        case .request(let id, _):
            return handleJSONRPCRequest(body, requestID: id, request: request)
        }
    }

    private func handleInitializationRequest(_ body: Data, request: HTTPRequest) -> HTTPResponse {
        // Reject re-initialization at the transport level
        if sessionID != nil {
            return .error(
                statusCode: 400,
                .invalidRequest("Bad Request: Session already initialized"),
                sessionID: sessionID
            )
        }

        // Generate session ID
        let newSessionID = sessionIDGenerator.generateSessionID()

        // Validate session ID contains only visible ASCII (0x21-0x7E)
        guard isValidSessionID(newSessionID) else {
            logger.error("Generated session ID contains invalid characters")
            return .error(
                statusCode: 500,
                .internalError("Internal error: Invalid session ID generated")
            )
        }

        self.sessionID = newSessionID
        logger.info("Session initialized", metadata: ["sessionID": "\(newSessionID)"])

        // Extract request ID for routing the response
        guard case .request(let requestID, _) = JSONRPCMessageKind(data: body) else {
            return .error(
                statusCode: 400,
                .parseError("Invalid initialize request"),
                sessionID: newSessionID
            )
        }

        // For the initialize request, use SSE streaming like any other request
        return handleJSONRPCRequest(body, requestID: requestID, request: request)
    }

    private func handleJSONRPCRequest(_ body: Data, requestID: String, request: HTTPRequest) -> HTTPResponse {
        // The message enters the server's queue before anything is held for it: a message the
        // queue cannot take leaves no stream, context or event behind. Nothing else runs on
        // this actor until this method returns, so the response cannot precede the priming
        // event in the store.
        guard enqueueIncoming(body) else { return incomingCapacityResponse() }

        // Create SSE stream for this request
        let (sseStream, slot) = openStream(requestID)
        httpRequestContexts[requestID] = request

        // Extract protocol version for priming event decision
        let protocolVersion = extractProtocolVersion(from: body, request: request)

        // Send priming event for resumability
        sendPrimingEvent(streamID: requestID, slot: slot, protocolVersion: protocolVersion)

        return .stream(sseStream, headers: sseHeaders())
    }

    /// Offers a client message to the server's queue; `false` when the bounded queue is full
    /// or the session has ended.
    private func enqueueIncoming(_ body: Data) -> Bool {
        switch incomingContinuation.yield(body) {
        case .enqueued:
            return true
        case .dropped, .terminated:
            return false
        @unknown default:
            return true
        }
    }

    private func incomingCapacityResponse() -> HTTPResponse {
        logger.warning("Incoming message queue is full; message refused")
        return .error(
            statusCode: 429,
            .internalError("Too Many Requests: the server's incoming message queue is full"),
            sessionID: sessionID
        )
    }

    // MARK: - GET Handler

    private func handleGet(_ request: HTTPRequest) -> HTTPResponse {
        // Build validation context (GET is never an initialization request)
        let context = HTTPValidationContext(
            httpMethod: "GET",
            sessionID: sessionID,
            isInitializationRequest: false,
            supportedProtocolVersions: Version.supported
        )

        // Run validation pipeline
        if let errorResponse = validationPipeline.validate(request, context: context) {
            return errorResponse
        }

        // Handle resumability: check for Last-Event-ID header
        if let lastEventID = request.header(HTTPHeaderName.lastEventID) {
            return handleResumeRequest(lastEventID: lastEventID, request: request)
        }

        // Only one standalone GET stream per session; a connection that ended freed its slot.
        guard standaloneStream == nil else {
            return .error(
                statusCode: 409,
                .invalidRequest("Conflict: Only one SSE stream is allowed per session"),
                sessionID: sessionID
            )
        }

        // Create standalone SSE stream
        let (sseStream, slot) = openStream(standaloneStreamID)

        // Extract protocol version for priming event
        let protocolVersion = request.header(HTTPHeaderName.protocolVersion) ?? Version.latest

        // Send priming event
        sendPrimingEvent(streamID: standaloneStreamID, slot: slot, protocolVersion: protocolVersion)

        return .stream(sseStream, headers: sseHeaders())
    }

    // MARK: - DELETE Handler

    private func handleDelete(_ request: HTTPRequest) -> HTTPResponse {
        // Validate session
        let context = HTTPValidationContext(
            httpMethod: "DELETE",
            sessionID: sessionID,
            isInitializationRequest: false,
            supportedProtocolVersions: Version.supported
        )

        if let errorResponse = validationPipeline.validate(request, context: context) {
            return errorResponse
        }

        terminate()

        return .ok(headers: sessionHeaders())
    }

    // MARK: - Message Routing

    /// Routes a message to a specific request's SSE stream without closing it.
    private func routeToRequestStream(_ data: Data, requestID: String, terminal: Bool) {
        let frame = storeMessage(streamID: requestID, data: data, terminal: terminal)

        guard let slot = requestStreams[requestID] else {
            logger.debug(
                "No active stream for request, message stored for replay",
                metadata: ["requestID": "\(requestID)"]
            )
            return
        }

        deliver(frame, to: slot, streamID: requestID)
    }

    /// Routes a response to a specific request's SSE stream and closes it.
    /// Used for final responses to client requests.
    private func routeResponse(_ data: Data, requestID: String) {
        routeToRequestStream(data, requestID: requestID, terminal: true)

        // Response means the request is complete — close the stream
        if let slot = requestStreams.removeValue(forKey: requestID) {
            slot.continuation.finish()
        }
        httpRequestContexts.removeValue(forKey: requestID)
    }

    private func routeServerInitiatedMessage(_ data: Data) {
        let frame = storeMessage(streamID: standaloneStreamID, data: data, terminal: false)

        guard let slot = standaloneStream else {
            logger.debug("No standalone GET stream connected, message stored for replay")
            return
        }

        deliver(frame, to: slot, streamID: standaloneStreamID)
    }

    // MARK: - Stream Slots

    /// Opens a connected stream under `streamID`, ending any older connection still registered
    /// there, and arranges for this connection's end to free exactly this slot.
    private func openStream(_ streamID: String) -> (AsyncThrowingStream<Data, Swift.Error>, StreamSlot) {
        streamGeneration += 1
        let generation = streamGeneration
        let (stream, continuation) = makeDeliveryStream()
        let slot = StreamSlot(continuation: continuation, generation: generation)
        if streamID == standaloneStreamID {
            standaloneStream?.continuation.finish()
            standaloneStream = slot
        } else {
            requestStreams[streamID]?.continuation.finish()
            requestStreams[streamID] = slot
        }
        continuation.onTermination = { [weak self] _ in
            guard let self else { return }
            Task { await self.streamDidEnd(streamID, generation: generation) }
        }
        return (stream, slot)
    }

    private func makeDeliveryStream() -> (AsyncThrowingStream<Data, Swift.Error>, AsyncThrowingStream<Data, Swift.Error>.Continuation) {
        guard let deliveryCapacity else {
            return AsyncThrowingStream<Data, Swift.Error>.makeStream()
        }
        return AsyncThrowingStream<Data, Swift.Error>.makeStream(
            bufferingPolicy: .bufferingOldest(deliveryCapacity)
        )
    }

    /// The end of one connection, however it ended: the slot it held, and only that slot, is
    /// freed for the next connection. A newer connection under the same stream keeps its slot.
    private func streamDidEnd(_ streamID: String, generation: UInt64) {
        if streamID == standaloneStreamID {
            if standaloneStream?.generation == generation {
                standaloneStream = nil
            }
        } else if requestStreams[streamID]?.generation == generation {
            requestStreams.removeValue(forKey: streamID)
        }
    }

    /// Offers one SSE frame to a connected stream in bounded chunks. A reader that cannot keep
    /// up ends that stream alone — the frame stays in the store for its resume — and a reader
    /// already gone frees its slot.
    private func deliver(_ frame: Data, to slot: StreamSlot, streamID: String) {
        guard offer(frame, to: slot.continuation) else {
            logger.warning(
                "SSE delivery queue overflow; stream ended for resumption",
                metadata: ["streamID": "\(streamID)"]
            )
            slot.continuation.finish(
                throwing: MCPError.internalError("SSE delivery queue overflow; resume with Last-Event-ID")
            )
            streamDidEnd(streamID, generation: slot.generation)
            return
        }
    }

    /// Enqueues a frame chunk by chunk; `false` when the queue is full or the reader is gone,
    /// with the stream left for the caller to end.
    private func offer(_ frame: Data, to continuation: AsyncThrowingStream<Data, Swift.Error>.Continuation) -> Bool {
        var offset = 0
        repeat {
            let end = min(offset + Self.deliveryChunkSize, frame.count)
            switch continuation.yield(frame.subdata(in: offset..<end)) {
            case .enqueued:
                offset = end
            case .dropped, .terminated:
                return false
            @unknown default:
                offset = end
            }
        } while offset < frame.count
        return true
    }

    // MARK: - Resumability

    private func handleResumeRequest(lastEventID: String, request: HTTPRequest) -> HTTPResponse {
        let streamID: String
        let frames: [Data]
        let completed: Bool
        switch replay(after: lastEventID) {
        case .unknown:
            return .error(
                statusCode: 400,
                .invalidRequest("Invalid Last-Event-ID"),
                sessionID: sessionID
            )
        case .evicted:
            return .error(
                statusCode: 400,
                .invalidRequest("Last-Event-ID has left the replay window; open a new stream without it"),
                sessionID: sessionID
            )
        case .events(let retainedStreamID, let retainedFrames, let retainedCompleted):
            streamID = retainedStreamID
            frames = retainedFrames
            completed = retainedCompleted
        }

        // A request stream whose response is retained is replayed and ended, as the original
        // ended when it answered; nothing is registered for it.
        if completed {
            let (sseStream, continuation) = makeDeliveryStream()
            for frame in frames where !offer(frame, to: continuation) {
                continuation.finish(
                    throwing: MCPError.internalError("SSE delivery queue overflow; resume with Last-Event-ID")
                )
                return .stream(sseStream, headers: sseHeaders())
            }
            continuation.finish()
            return .stream(sseStream, headers: sseHeaders())
        }

        // Re-register the stream for future messages, replaying what it missed
        let (sseStream, slot) = openStream(streamID)
        for frame in frames {
            deliver(frame, to: slot, streamID: streamID)
        }

        // Send a new priming event so the client can resume again if disconnected
        let protocolVersion = request.header(HTTPHeaderName.protocolVersion) ?? Version.latest
        sendPrimingEvent(streamID: streamID, slot: slot, protocolVersion: protocolVersion)

        return .stream(sseStream, headers: sseHeaders())
    }

    // MARK: - Internal Event Store

    private enum Replay {
        /// The retained events after the cursor on its own stream, and whether that stream's
        /// response is among the retained events.
        case events(streamID: String, frames: [Data], completed: Bool)
        /// The cursor named an event this session issued that the window no longer holds.
        case evicted
        case unknown
    }

    private func nextEventID(for streamID: String) -> String {
        eventCounter += 1
        return "\(streamID)_\(eventCounter)"
    }

    /// Stores a message and returns its SSE frame.
    private func storeMessage(streamID: String, data: Data, terminal: Bool) -> Data {
        let eventID = nextEventID(for: streamID)
        let frame = SSEEvent.message(data: data, id: eventID).formatted()
        retain(StoredEvent(streamID: streamID, eventID: eventID, frame: frame, terminal: terminal))
        return frame
    }

    /// Retains an event, evicting the oldest while the window is over either bound. The event
    /// just stored always stays, so the ID the client is about to receive is a valid cursor.
    private func retain(_ event: StoredEvent) {
        storedEvents.append(event)
        storedBytes += event.retainedBytes
        guard let replayRetention else { return }
        while storedEvents.count > 1,
            storedEvents.count > replayRetention.maxEvents || storedBytes > replayRetention.maxBytes
        {
            storedBytes -= storedEvents.removeFirst().retainedBytes
        }
    }

    private func replay(after lastEventID: String) -> Replay {
        guard let index = storedEvents.firstIndex(where: { $0.eventID == lastEventID }) else {
            return wasIssued(lastEventID) ? .evicted : .unknown
        }
        let streamID = storedEvents[index].streamID
        let retained = storedEvents[index...].filter { $0.streamID == streamID }
        return .events(
            streamID: streamID,
            frames: retained.dropFirst().compactMap(\.frame),
            completed: retained.contains(where: \.terminal)
        )
    }

    /// Whether an ID names an event this session issued: its counter, after the last
    /// underscore — a request ID may hold underscores of its own — is one the store has reached.
    private func wasIssued(_ eventID: String) -> Bool {
        guard let separator = eventID.lastIndex(of: "_"),
            let counter = Int(eventID[eventID.index(after: separator)...])
        else { return false }
        return counter >= 1 && counter <= eventCounter
    }

    // MARK: - SSE Helpers

    private func sendPrimingEvent(streamID: String, slot: StreamSlot, protocolVersion: String) {
        // Priming events with empty data are only safe for clients >= 2025-03-26
        guard protocolVersion >= "2025-03-26" else { return }

        let primingEventID = nextEventID(for: streamID)
        retain(StoredEvent(streamID: streamID, eventID: primingEventID, frame: nil, terminal: false))
        let primingEvent = SSEEvent.priming(id: primingEventID, retry: retryInterval)
        deliver(primingEvent.formatted(), to: slot, streamID: streamID)
    }

    private func sseHeaders() -> [String: String] {
        var headers = sessionHeaders()
        headers[HTTPHeaderName.contentType] = ContentType.sse
        headers[HTTPHeaderName.cacheControl] = "no-cache, no-transform"
        headers[HTTPHeaderName.connection] = "keep-alive"
        return headers
    }

    private func extractProtocolVersion(from body: Data, request: HTTPRequest) -> String {
        // For initialize requests, extract from the request body params
        if let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
           let method = json["method"] as? String, method == "initialize",
           let params = json["params"] as? [String: Any],
           let version = params["protocolVersion"] as? String
        {
            return version
        }
        // For other requests, use the header
        return request.header(HTTPHeaderName.protocolVersion) ?? Version.latest
    }

    // MARK: - Session Helpers

    private func sessionHeaders() -> [String: String] {
        var headers: [String: String] = [:]
        if let sessionID {
            headers[HTTPHeaderName.sessionID] = sessionID
        }
        return headers
    }

    private func isValidSessionID(_ id: String) -> Bool {
        guard !id.isEmpty else { return false }
        return id.utf8.allSatisfy { $0 >= 0x21 && $0 <= 0x7E }
    }

    // MARK: - Stream Control

    /// Closes the SSE stream for a specific request without sending a response.
    ///
    /// Used to trigger client reconnection mid-call (SEP-1699). The response,
    /// when eventually sent, will be stored and replayed to the reconnected stream.
    package func closeSSEStream(forRequestID requestID: String) {
        guard let slot = requestStreams.removeValue(forKey: requestID) else { return }
        slot.continuation.finish()
        httpRequestContexts.removeValue(forKey: requestID)
    }

    // MARK: - HTTPContextProviding

    public func httpRequestContext(for id: ID) -> HTTPRequest? {
        httpRequestContexts[id.description]
    }

    // MARK: - RequestSettlementObserving

    /// A response closed its stream as it was routed; a request settled without one — cancelled,
    /// or failed before a response — ends its stream here, so its POST does not wait out a
    /// deadline, and its HTTP context goes either way.
    public func requestDidSettle(id: ID, outcome: RequestSettlement) {
        let requestID = id.description
        httpRequestContexts.removeValue(forKey: requestID)
        guard outcome != .responded, let slot = requestStreams.removeValue(forKey: requestID) else {
            return
        }
        slot.continuation.finish()
    }

    // MARK: - Termination

    /// Terminates the session, closing all active streams.
    /// After termination, all requests receive 404 Not Found.
    private func terminate() {
        guard !terminated else { return }
        terminated = true

        logger.info("Terminating session", metadata: ["sessionID": "\(sessionID ?? "none")"])

        // Close all request SSE streams
        for slot in requestStreams.values {
            slot.continuation.finish()
        }
        requestStreams.removeAll()
        httpRequestContexts.removeAll()

        // Close standalone GET stream
        standaloneStream?.continuation.finish()
        standaloneStream = nil

        // Clear stored events
        storedEvents.removeAll()
        storedBytes = 0

        // Close incoming stream
        incomingContinuation.finish()
    }
}
