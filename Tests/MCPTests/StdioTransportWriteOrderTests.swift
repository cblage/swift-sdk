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

@Suite("Stdio Transport Write Order Tests", .timeLimit(.minutes(1)))
struct StdioTransportWriteOrderTests {
    @Test("Concurrent large messages stay whole under pipe backpressure")
    func testConcurrentLargeMessages() async throws {
        try await withTransport { transport, pipes in
            let first = Data(repeating: 65, count: 512 * 1024)
            let second = Data(repeating: 66, count: 512 * 1024)
            let (firstSend, _) = await startSend(first, on: transport)

            // Drain the prefix and enter the second send on the same actor turn.
            // The first send cannot refill the pipe between these two operations.
            let (secondSend, prefix) = await startSend(
                second, on: transport, draining: pipes.reader)
            try #require(!prefix.isEmpty && prefix.count < first.count)
            #expect(prefix.allSatisfy { $0 == 65 })

            let output = try await readOutput(
                from: pipes.reader, count: first.count + second.count + 2, prefix: prefix)
            try await firstSend.value
            try await secondSend.value
            expectMessages(output, equalTo: [first, second])
        }
    }

    @Test("Queued small sends keep arrival order and each arrive once")
    func testQueuedSendOrder() async throws {
        try await withTransport { transport, pipes in
            let first = Data(repeating: 65, count: 512 * 1024)
            let (firstSend, _) = await startSend(first, on: transport)
            var sends = [firstSend]
            var messages = [first]
            for index in 0..<32 {
                let message = Data("message-\(index)".utf8)
                let (send, _) = await startSend(message, on: transport)
                sends.append(send)
                messages.append(message)
            }

            let output = try await readOutput(
                from: pipes.reader, count: messages.reduce(0) { $0 + $1.count + 1 })
            for send in sends {
                try await send.value
            }
            expectMessages(output, equalTo: messages)
        }
    }

    @Test("Cancelling a waiting send releases it and preserves later sends and receive")
    func testCancelledWaitingSend() async throws {
        try await withTransport { transport, pipes in
            let first = Data(repeating: 65, count: 512 * 1024)
            let last = Data("last".utf8)
            let (firstSend, _) = await startSend(first, on: transport)
            let (cancelledSend, _) = await startSend(Data("cancelled".utf8), on: transport)
            let (lastSend, _) = await startSend(last, on: transport)

            cancelledSend.cancel()
            do {
                try await cancelledSend.value
                Issue.record("Expected the queued send to throw CancellationError")
            } catch is CancellationError {
            }

            // Receiving remains possible while the output pipe is full.
            try pipes.writer.writeAll(Data("incoming\n".utf8))
            var iterator = await transport.receive().makeAsyncIterator()
            let received = try await iterator.next()
            #expect(received == Data("incoming".utf8))

            let output = try await readOutput(
                from: pipes.reader, count: first.count + last.count + 2)
            try await firstSend.value
            try await lastSend.value
            expectMessages(output, equalTo: [first, last])
        }
    }

    @Test("Cancellation after the first byte still completes the line")
    func testCancelledPartialSend() async throws {
        try await withTransport { transport, pipes in
            let first = Data(repeating: 65, count: 512 * 1024)
            let second = Data("after-cancellation".utf8)
            let (firstSend, _) = await startSend(first, on: transport)
            let (secondSend, prefix) = await startSend(
                second, on: transport, draining: pipes.reader)
            try #require(!prefix.isEmpty && prefix.count < first.count)
            firstSend.cancel()

            let output = try await readOutput(
                from: pipes.reader, count: first.count + second.count + 2, prefix: prefix)
            try await firstSend.value
            try await secondSend.value
            expectMessages(output, equalTo: [first, second])
        }
    }

    @Test("A cancelled task still sends when the output has room")
    func testCancelledSendWithFreeOutput() async throws {
        try await withTransport { transport, pipes in
            let first = Data("cancelled".utf8)
            let cancelledSend = Task {
                withUnsafeCurrentTask { $0?.cancel() }
                try await transport.send(first)
            }
            try await cancelledSend.value

            let message = Data("next".utf8)
            try await transport.send(message)
            let output = try readAvailable(from: pipes.reader)
            expectMessages(output, equalTo: [first, message])
        }
    }

    @Test("Cancellation while waiting for the first byte releases the next send")
    func testCancelledBeforeFirstByteOnFullPipe() async throws {
        try await withTransport { transport, pipes in
            var filler = Data(repeating: 70, count: 4096)
            var filledCount = 0
            while true {
                do {
                    filledCount += try filler.withUnsafeBytes { try pipes.output.write($0) }
                } catch let error where MCPError.isResourceTemporarilyUnavailable(error) {
                    // Fill any space smaller than a whole buffer, down to the last byte.
                    if filler.count == 1 { break }
                    filler = Data([70])
                }
            }
            try #require(filledCount > 0)

            let (cancelledSend, _) = await startSend(Data("cancelled".utf8), on: transport)
            let message = Data("next".utf8)
            let (nextSend, _) = await startSend(message, on: transport)
            cancelledSend.cancel()
            do {
                try await cancelledSend.value
                Issue.record("Expected cancellation while the first byte cannot be written")
            } catch is CancellationError {
            }

            let output = try await readOutput(
                from: pipes.reader, count: filledCount + message.count + 1)
            try await nextSend.value
            #expect(output.prefix(filledCount).allSatisfy { $0 == 70 })
            expectMessages(Data(output.dropFirst(filledCount)), equalTo: [message])
        }
    }

    @Test("Disconnect releases queued sends and prevents reuse of an incomplete line")
    func testDisconnectWithQueuedSends() async throws {
        try await withTransport { transport, pipes in
            let first = Data(repeating: 65, count: 512 * 1024)
            let (firstSend, _) = await startSend(first, on: transport)
            let (secondSend, _) = await startSend(Data("second".utf8), on: transport)
            let (thirdSend, _) = await startSend(Data("third".utf8), on: transport)

            await transport.disconnect()
            for send in [firstSend, secondSend, thirdSend] {
                do {
                    try await send.value
                    Issue.record("Expected ENOTCONN for an unfinished send")
                } catch {
                    expectNotConnected(error)
                }
            }
            #expect(fcntl(pipes.input.rawValue, F_GETFL) & O_NONBLOCK == 0)
            #expect(fcntl(pipes.output.rawValue, F_GETFL) & O_NONBLOCK == 0)
            let prefix = try readAvailable(from: pipes.reader)
            #expect(!prefix.isEmpty && prefix.count < first.count)
            #expect(prefix.allSatisfy { $0 == 65 })

            do {
                try await transport.connect()
                Issue.record("Expected an incomplete output line to prevent reconnection")
            } catch {
                expectNotConnected(error)
            }
            do {
                try await transport.send(Data("after-disconnect".utf8))
                Issue.record("Expected the disconnected transport to reject another message")
            } catch {
                expectNotConnected(error)
            }
            #expect(try readAvailable(from: pipes.reader).isEmpty)
        }
    }

    @Test("A clean disconnect permits reconnection")
    func testReconnectAfterCleanDisconnect() async throws {
        try await withTransport { transport, pipes in
            let first = Data("before-disconnect".utf8)
            let second = Data("after-reconnect".utf8)
            try await transport.send(first)
            await transport.disconnect()
            try await transport.connect()
            try await transport.send(second)
            let output = try readAvailable(from: pipes.reader)
            expectMessages(output, equalTo: [first, second])
        }
    }

    @Test("A write error before the first byte releases the next send")
    func testWriteErrorBeforeWriting() async throws {
        try await withTransport { transport, pipes in
            let savedOutput = dup(pipes.output.rawValue)
            try #require(savedOutput >= 0)
            defer { _ = close(savedOutput) }
            try #require(dup2(pipes.input.rawValue, pipes.output.rawValue) == pipes.output.rawValue)
            do {
                try await transport.send(Data("failed".utf8))
                Issue.record("Expected writing to a read descriptor to fail")
            } catch {
                expectTransportError(error, code: EBADF)
            }

            try #require(dup2(savedOutput, pipes.output.rawValue) == pipes.output.rawValue)
            let message = Data("next".utf8)
            try await transport.send(message)
            let output = try readAvailable(from: pipes.reader)
            expectMessages(output, equalTo: [message])
        }
    }

    @Test("A write error after a partial line closes the transport and releases waiters")
    func testWriteErrorAfterPartialWrite() async throws {
        try await withTransport { transport, pipes in
            let first = Data(repeating: 65, count: 512 * 1024)
            let (firstSend, _) = await startSend(first, on: transport)
            let (secondSend, _) = await startSend(Data("second".utf8), on: transport)
            try #require(dup2(pipes.input.rawValue, pipes.output.rawValue) == pipes.output.rawValue)
            do {
                try await firstSend.value
                Issue.record("Expected the partial write to fail")
            } catch {
                expectTransportError(error, code: EBADF)
            }
            do {
                try await secondSend.value
                Issue.record("Expected the waiting send to fail on disconnect")
            } catch {
                expectNotConnected(error)
            }

            await transport.disconnect()
            let prefix = try readAvailable(from: pipes.reader)
            #expect(!prefix.isEmpty && prefix.count < first.count)
            #expect(prefix.allSatisfy { $0 == 65 })
            do {
                try await transport.connect()
                Issue.record("Expected the incomplete line to prevent reconnection")
            } catch {
                expectNotConnected(error)
            }
        }
    }

    private func startSend(
        _ message: Data, on transport: isolated StdioTransport,
        draining reader: FileDescriptor? = nil
    ) async -> (Task<Void, Swift.Error>, Data) {
        let (started, continuation) = AsyncStream<Data>.makeStream()
        let send = Task {
            defer { continuation.finish() }
            let prefix = try reader.map { try readAvailable(from: $0) } ?? Data()
            continuation.yield(prefix)
            try await transport.send(message)
        }
        var iterator = started.makeAsyncIterator()
        let prefix = await iterator.next() ?? Data()
        return (send, prefix)
    }

    private func expectMessages(_ output: Data, equalTo messages: [Data]) {
        #expect(output.last == UInt8(ascii: "\n"))
        let lines = output.split(separator: UInt8(ascii: "\n")).map { Data($0) }
        #expect(lines.count == messages.count)
        let wholeMessagesInOrder = lines == messages
        #expect(wholeMessagesInOrder, "Each line must equal one whole message in arrival order")
    }

    private func expectNotConnected(_ error: Swift.Error) {
        expectTransportError(error, code: ENOTCONN)
    }

    private func expectTransportError(_ error: Swift.Error, code: CInt) {
        guard let error = error as? MCPError, case .transportError(let underlying) = error else {
            Issue.record("Expected MCPError.transportError, got \(error)")
            return
        }
        #expect((underlying as? Errno)?.rawValue == code)
    }

    private func readAvailable(from reader: FileDescriptor) throws -> Data {
        var output = Data()
        var buffer = [UInt8](repeating: 0, count: 4096)
        while true {
            do {
                let count = try buffer.withUnsafeMutableBytes { try reader.read(into: $0) }
                if count == 0 { return output }
                output.append(contentsOf: buffer[..<count])
            } catch let error where MCPError.isResourceTemporarilyUnavailable(error) {
                return output
            }
        }
    }

    private func readOutput(from reader: FileDescriptor, count: Int, prefix: Data = Data())
        async throws -> Data
    {
        var output = prefix
        let deadline = ContinuousClock.now + .seconds(10)
        while output.count < count && ContinuousClock.now < deadline {
            output.append(try readAvailable(from: reader))
            if output.count < count {
                try await Task.sleep(for: .milliseconds(1))
            }
        }
        try #require(output.count == count, "All message bytes must arrive before the deadline")
        return output
    }

    private func withTransport(
        _ operation: (StdioTransport, Pipes) async throws -> Void
    ) async throws {
        let pipes = try Pipes()
        defer { pipes.close() }
        let transport = StdioTransport(input: pipes.input, output: pipes.output)
        do {
            try await transport.connect()
            try await operation(transport, pipes)
        } catch {
            await transport.disconnect()
            throw error
        }
        await transport.disconnect()
    }

    private struct Pipes {
        let input: FileDescriptor
        let writer: FileDescriptor
        let reader: FileDescriptor
        let output: FileDescriptor

        init() throws {
            (input, writer) = try FileDescriptor.pipe()
            do {
                (reader, output) = try FileDescriptor.pipe()
            } catch {
                try? input.close()
                try? writer.close()
                throw error
            }
            let flags = fcntl(reader.rawValue, F_GETFL)
            guard flags >= 0, fcntl(reader.rawValue, F_SETFL, flags | O_NONBLOCK) == 0 else {
                let error = Errno(rawValue: CInt(errno))
                close()
                throw error
            }
        }

        func close() {
            try? input.close()
            try? writer.close()
            try? reader.close()
            try? output.close()
        }
    }
}
