# frozen_string_literal: true

require "json"
require "socket"

# A loopback HTTP/1.1 server standing in for the JSON door, so the client is exercised through
# real Net::HTTP. Records each request; answers with whatever the test's handler returns.
class FakeServer
  Request = Data.define(:verb, :path, :headers, :body) do
    def json = JSON.parse(body)
  end

  attr_reader :requests

  # handler: ->(request) { [status, body] }, body a Hash (sent as JSON), a String (sent raw), or
  # an Array or Proc (sent as an event stream; see #stream).
  def initialize(&handler)
    @handler = handler
    @requests = Queue.new
    @server = TCPServer.new("127.0.0.1", 0)
    @thread = Thread.new { serve }
  end

  def url = "http://127.0.0.1:#{@server.addr[1]}"

  def last_request = @requests.pop(timeout: 5)

  def stop
    @server.close
    @thread.join(5)
  end

  private

  def serve
    loop do
      socket = @server.accept
      handle(socket)
    ensure
      socket&.close
    end
  rescue IOError, Errno::EBADF
    nil
  end

  def handle(socket)
    request = read_request(socket) or return
    @requests << request
    status, body = @handler.call(request)
    return stream(socket, status, body) if body.is_a?(Array) || body.is_a?(Proc)

    payload = body.is_a?(String) ? body : JSON.generate(body)
    socket.write("HTTP/1.1 #{status} X\r\nContent-Type: application/json\r\n" \
                 "Content-Length: #{payload.bytesize}\r\nConnection: close\r\n\r\n#{payload}")
  rescue Errno::EPIPE, Errno::ECONNRESET
    nil # client hung up mid-stream
  end

  # Unsized event stream ended by close. Array: chunks written in turn. Proc: given the socket.
  def stream(socket, status, body)
    socket.write("HTTP/1.1 #{status} X\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n")
    return body.call(socket) if body.is_a?(Proc)

    body.each do |chunk|
      socket.write(chunk)
      socket.flush
    end
  end

  def read_request(socket)
    line = socket.gets or return nil
    verb, path = line.split
    headers = {}
    while (header = socket.gets) && header != "\r\n"
      name, value = header.split(":", 2)
      headers[name.downcase] = value.strip
    end
    body = socket.read(headers.fetch("content-length", "0").to_i)
    Request.new(verb: verb, path: path, headers: headers, body: body)
  end
end
