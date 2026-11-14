import Foundation
import Testing

#if canImport(Darwin)
    import Darwin.POSIX
#elseif canImport(Glibc)
    import Glibc
#elseif canImport(Musl)
    import Musl
#endif

@testable import MCP

#if canImport(System)
    import System
#else
    @preconcurrency import SystemPackage
#endif

@Suite("Stdio Transport Socket Tests", .timeLimit(.minutes(1)))
struct StdioTransportSocketTests {
    @Test("Socket pairs connect, exchange messages, and restore blocking flags")
    func testSocketConnectionAndFlagRestoration() async throws {
        try await withSockets { transport, sockets in
            let inputFlags = fcntl(sockets.input.rawValue, F_GETFL)
            let outputFlags = fcntl(sockets.output.rawValue, F_GETFL)
            try #require(inputFlags >= 0 && inputFlags & O_NONBLOCK == 0)
            try #require(outputFlags >= 0 && outputFlags & O_NONBLOCK == 0)

            try await transport.connect()
            #expect(fcntl(sockets.input.rawValue, F_GETFL) & O_NONBLOCK != 0)
            #expect(fcntl(sockets.output.rawValue, F_GETFL) & O_NONBLOCK != 0)

            let incoming = Data(#"{"message":"incoming"}"#.utf8)
            try sockets.inputPeer.writeAll(incoming + Data([UInt8(ascii: "\n")]))
            var iterator = await transport.receive().makeAsyncIterator()
            let received = try await iterator.next()
            #expect(received == incoming)

            let outgoing = Data(#"{"message":"outgoing"}"#.utf8)
            try await transport.send(outgoing)
            let output = try await readOutput(from: sockets.outputPeer, count: outgoing.count + 1)
            #expect(output == outgoing + Data([UInt8(ascii: "\n")]))

            await transport.disconnect()
            #expect(fcntl(sockets.input.rawValue, F_GETFL) & O_NONBLOCK == 0)
            #expect(fcntl(sockets.output.rawValue, F_GETFL) & O_NONBLOCK == 0)
        }
    }

    @Test("A large message stays whole with socket backpressure and a slow reader")
    func testLargeSocketMessageToSlowReader() async throws {
        try await withSockets { transport, sockets in
            var bufferSize: CInt = 8192
            var optionSize = socklen_t(MemoryLayout<CInt>.size)
            try #require(
                setsockopt(sockets.output.rawValue, SOL_SOCKET, SO_SNDBUF, &bufferSize, optionSize)
                    == 0)
            try #require(
                getsockopt(sockets.output.rawValue, SOL_SOCKET, SO_SNDBUF, &bufferSize, &optionSize)
                    == 0)
            try #require(bufferSize > 0)

            let outputFlags = fcntl(sockets.output.rawValue, F_GETFL)
            try #require(outputFlags >= 0)
            try #require(
                fcntl(sockets.output.rawValue, F_SETFL, outputFlags | O_NONBLOCK) == 0)
            try await transport.connect()

            let message = Data(repeating: 65, count: max(512 * 1024, Int(bufferSize) + 1))
            let send = await startSend(message, on: transport)
            let output = try await readOutput(from: sockets.outputPeer, count: message.count + 1)
            try await send.value
            let wholeMessage = output == message + Data([UInt8(ascii: "\n")])
            #expect(wholeMessage, "The socket must carry the complete message and its newline")

            await transport.disconnect()
            #expect(fcntl(sockets.input.rawValue, F_GETFL) & O_NONBLOCK == 0)
            #expect(fcntl(sockets.output.rawValue, F_GETFL) & O_NONBLOCK != 0)
        }
    }

    private func startSend(_ message: Data, on transport: isolated StdioTransport) async
        -> Task<Void, Swift.Error>
    {
        let (started, continuation) = AsyncStream<Void>.makeStream()
        let send = Task {
            continuation.yield(())
            continuation.finish()
            try await transport.send(message)
        }
        var iterator = started.makeAsyncIterator()
        _ = await iterator.next()
        return send
    }

    private func readOutput(from reader: FileDescriptor, count: Int) async throws -> Data {
        var output = Data()
        var buffer = [UInt8](repeating: 0, count: 1024)
        let deadline = ContinuousClock.now + .seconds(10)
        while output.count < count && ContinuousClock.now < deadline {
            do {
                let bytesRead = try buffer.withUnsafeMutableBytes { try reader.read(into: $0) }
                if bytesRead == 0 { break }
                output.append(contentsOf: buffer[..<bytesRead])
            } catch let error where MCPError.isResourceTemporarilyUnavailable(error) {
                // A non-blocking reader waits for the next output chunk.
            }
            if output.count < count {
                // Pace each read so a large send must retain its remaining bytes across waits.
                try await Task.sleep(for: .milliseconds(1))
            }
        }
        try #require(
            output.count == count, "All socket message bytes must arrive before the deadline")
        return output
    }

    private func withSockets(
        _ operation: (StdioTransport, Sockets) async throws -> Void
    ) async throws {
        let sockets = try Sockets()
        defer { sockets.close() }
        let transport = StdioTransport(input: sockets.input, output: sockets.output)
        do {
            try await operation(transport, sockets)
        } catch {
            await transport.disconnect()
            throw error
        }
        await transport.disconnect()
    }

    private struct Sockets {
        let input: FileDescriptor
        let inputPeer: FileDescriptor
        let output: FileDescriptor
        let outputPeer: FileDescriptor

        init() throws {
            (input, inputPeer) = try Self.makeSocketPair()
            do {
                (output, outputPeer) = try Self.makeSocketPair()
            } catch {
                try? input.close()
                try? inputPeer.close()
                throw error
            }
            let flags = fcntl(outputPeer.rawValue, F_GETFL)
            guard flags >= 0, fcntl(outputPeer.rawValue, F_SETFL, flags | O_NONBLOCK) == 0 else {
                let error = Errno(rawValue: CInt(errno))
                close()
                throw error
            }
        }

        private static func makeSocketPair() throws -> (FileDescriptor, FileDescriptor) {
            #if canImport(Glibc)
                let socketType = CInt(SOCK_STREAM.rawValue)
            #else
                let socketType = SOCK_STREAM
            #endif
            var descriptors: [CInt] = [-1, -1]
            guard socketpair(AF_UNIX, socketType, 0, &descriptors) == 0 else {
                throw Errno(rawValue: CInt(errno))
            }
            return (
                FileDescriptor(rawValue: descriptors[0]),
                FileDescriptor(rawValue: descriptors[1])
            )
        }

        func close() {
            try? input.close()
            try? inputPeer.close()
            try? output.close()
            try? outputPeer.close()
        }
    }
}
