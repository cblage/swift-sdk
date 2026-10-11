import Foundation
import Logging
import Testing

@testable import MCP

/// A transport over an in-memory one that records how the server settles each request.
private actor SettlementRecorder: Transport, RequestSettlementObserving {
    nonisolated let logger: Logger
    private let underlying: InMemoryTransport
    private var incoming: AsyncThrowingStream<Data, Swift.Error>?
    private var settlements: [(id: ID, outcome: RequestSettlement)] = []

    init(underlying: InMemoryTransport) {
        self.underlying = underlying
        self.logger = underlying.logger
    }

    func connect() async throws {
        try await underlying.connect()
        incoming = await underlying.receive()
    }

    func disconnect() async {
        await underlying.disconnect()
    }

    func send(_ data: Data) async throws {
        try await underlying.send(data)
    }

    func receive() -> AsyncThrowingStream<Data, Swift.Error> {
        incoming ?? AsyncThrowingStream { $0.finish() }
    }

    func requestDidSettle(id: ID, outcome: RequestSettlement) {
        settlements.append((id, outcome))
    }

    func settlements(of id: ID) -> [RequestSettlement] {
        settlements.filter { $0.id == id }.map(\.outcome)
    }
}

private func makeRawRequest(id: Int, method: String) -> Data {
    Data("{\"jsonrpc\":\"2.0\",\"id\":\(id),\"method\":\"\(method)\"}".utf8)
}

/// Waits up to a second for the recorder to hold a settlement of `id`.
private func settlements(of id: ID, in recorder: SettlementRecorder) async -> [RequestSettlement] {
    for _ in 0..<100 {
        let settled = await recorder.settlements(of: id)
        if !settled.isEmpty { return settled }
        try? await Task.sleep(for: .milliseconds(10))
    }
    return []
}

@Suite("Request Settlement Tests")
struct RequestSettlementTests {
    @Test("An answered request settles once as responded")
    func testAnsweredRequestSettlesResponded() async throws {
        let (clientTransport, serverTransport) = await InMemoryTransport.createConnectedPair()
        let recorder = SettlementRecorder(underlying: serverTransport)
        let client = Client(name: "TestClient", version: "1.0")
        let server = Server(name: "TestServer", version: "1.0", capabilities: .init())
        try await server.start(transport: recorder)
        _ = try await client.connect(transport: clientTransport)

        let context = try await client.send(Ping.request())
        _ = try await context.value

        let settled = await settlements(of: context.requestID, in: recorder)
        #expect(settled == [.responded])

        await client.disconnect()
        await server.stop()
    }

    @Test("A cancelled request settles once as cancelled, with no response sent")
    func testCancelledRequestSettlesCancelled() async throws {
        let (clientTransport, serverTransport) = await InMemoryTransport.createConnectedPair()
        let recorder = SettlementRecorder(underlying: serverTransport)
        let client = Client(name: "TestClient", version: "1.0")
        let server = Server(name: "TestServer", version: "1.0", capabilities: .init())
        try await server.start(transport: recorder)
        await server.withMethodHandler(Ping.self) { _ in
            try await Task.sleep(for: .seconds(10))
            return Empty()
        }
        _ = try await client.connect(transport: clientTransport)

        let context = try await client.send(Ping.request())
        try await Task.sleep(for: .milliseconds(50))
        try await client.cancelRequest(context.requestID, reason: "Test cancellation")

        let settled = await settlements(of: context.requestID, in: recorder)
        #expect(settled == [.cancelled])
        do {
            _ = try await context.value
            Issue.record("A cancelled request received a response")
        } catch is CancellationError {
        } catch {
            Issue.record("Expected CancellationError but got: \(error)")
        }

        await client.disconnect()
        await server.stop()
    }

    @Test("A request refused before dispatch settles as failed")
    func testRefusedRequestSettlesFailed() async throws {
        // A strict server refuses a listing before initialization by throwing, sending nothing.
        let (clientTransport, serverTransport) = await InMemoryTransport.createConnectedPair()
        let recorder = SettlementRecorder(underlying: serverTransport)
        let server = Server(
            name: "TestServer", version: "1.0", capabilities: .init(), configuration: .strict)
        try await server.start(transport: recorder)
        try await clientTransport.connect()

        try await clientTransport.send(makeRawRequest(id: 7, method: "tools/list"))

        let settled = await settlements(of: .number(7), in: recorder)
        #expect(settled == [.failed])

        await server.stop()
    }

    @Test("A request the server cannot decode settles as responded, with its error")
    func testUndecodableRequestSettlesResponded() async throws {
        let (clientTransport, serverTransport) = await InMemoryTransport.createConnectedPair()
        let recorder = SettlementRecorder(underlying: serverTransport)
        let server = Server(name: "TestServer", version: "1.0", capabilities: .init())
        try await server.start(transport: recorder)
        try await clientTransport.connect()

        try await clientTransport.send(Data("{\"jsonrpc\":\"2.0\",\"id\":8,\"method\":5}".utf8))

        let settled = await settlements(of: .number(8), in: recorder)
        #expect(settled == [.responded])

        await server.stop()
    }
}
