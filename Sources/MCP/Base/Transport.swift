import Logging

import struct Foundation.Data

/// Protocol defining the transport layer for MCP communication
public protocol Transport: Actor {
    var logger: Logger { get }

    /// Establishes connection with the transport
    func connect() async throws

    /// Disconnects from the transport
    func disconnect() async

    /// Sends data
    func send(_ data: Data) async throws

    /// Receives data in an async sequence
    func receive() -> AsyncThrowingStream<Data, Swift.Error>
}

/// How the server settled one request it received.
public enum RequestSettlement: Sendable, Equatable {
    /// Its response, a result or an error, was handed to the transport.
    case responded
    /// It was cancelled by the client's notification; no response is sent.
    case cancelled
    /// Its dispatch failed before a response could be produced.
    case failed
}

/// A transport the server tells, once, when each request it delivered has settled — whether or
/// not a response was ever sent — so the transport can release what it holds for the request:
/// a response stream, the originating HTTP context, an admission slot.
public protocol RequestSettlementObserving: Sendable {
    func requestDidSettle(id: ID, outcome: RequestSettlement) async
}
