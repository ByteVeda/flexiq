# frozen_string_literal: true

require "json"
require "net/http"
require "openssl"
require "uri"

module FlexiQ
  # HTTP/JSON to the server's JSON door: bearer auth, TLS, and failure mapping.
  #
  # One keep-alive connection, serialised by a mutex. Never retries: a resent write may land
  # twice, and only the caller (holding a `unique_key`) can make a resend safe.
  class Transport
    # Raised by Net::HTTP for anything short of an HTTP answer.
    NETWORK_ERRORS = [
      IOError, SystemCallError, SocketError, Timeout::Error, OpenSSL::SSL::SSLError, Net::HTTPBadResponse
    ].freeze
    private_constant :NETWORK_ERRORS

    EVENT_STREAM = "text/event-stream"
    private_constant :EVENT_STREAM

    # `tls:` keys: :ca_file, :ca_path, :cert (OpenSSL::X509::Certificate), :key (OpenSSL::PKey).
    # `watch_read_timeout`: silence before a watch counts as dropped; keep above the 15 s keepalive.
    def initialize(url, token:, insecure: false, tls: {}, open_timeout: 5, read_timeout: 30,
                   write_timeout: 30, watch_read_timeout: 60, user_agent: nil)
      @uri = parse_url(url, insecure)
      @authorization = "Bearer #{validate_token(token)}".freeze
      @user_agent = [user_agent, "flexiq-ruby/#{VERSION}"].compact.join(" ").freeze
      @tls = tls
      @open_timeout = open_timeout
      @write_timeout = write_timeout
      @watch_read_timeout = watch_read_timeout
      @http = build_http(read_timeout)
      @lock = Mutex.new
    end

    def get(path, query = {})
      perform(Net::HTTP::Get.new(full_path(with_query(path, query))))
    end

    # Yields an event stream's body in chunks until the server closes it. Own connection, so it
    # never blocks unary calls. `deadline` (Deadline) caps every read.
    def stream(path, query = {}, deadline: nil)
      request = Net::HTTP::Get.new(full_path(with_query(path, query)))
      authorize(request, EVENT_STREAM)
      # Compression would buffer events.
      request["Accept-Encoding"] = "identity"
      http = build_http(read_timeout_until(deadline))
      http.open_timeout = deadline.cap(@open_timeout) if deadline
      # Older net-http resends an idempotent GET after a mid-body error, restarting the stream
      # under the caller's parser. The watch decides when to reopen.
      http.max_retries = 0
      http.start do
        http.request(request) do |response|
          expect_event_stream(response)
          response.read_body do |chunk|
            http.read_timeout = read_timeout_until(deadline)
            yield chunk
          end
        end
      end
    rescue *NETWORK_ERRORS => e
      raise TransportError, "GET #{request.path}: #{e.class}: #{e.message}"
    end

    def post(path, body = nil)
      request = Net::HTTP::Post.new(full_path(path))
      request["Content-Type"] = "application/json"
      request.body = body.nil? ? "" : JSON.generate(body)
      perform(request)
    end

    def close
      @lock.synchronize { @http.finish if @http.started? }
    end

    private

    def parse_url(url, insecure)
      uri = URI.parse(url.to_s)
      raise ConfigurationError, "server URL must be http(s)://host[:port], got #{url.inspect}" unless uri.host
      raise ConfigurationError, "server URL must not carry a query or fragment" if uri.query || uri.fragment

      case uri.scheme
      when "https" then uri
      when "http"
        return uri if insecure

        raise ConfigurationError, "refusing to send a bearer token over plaintext http://; " \
                                  "use https://, or pass insecure: true for a loopback or socket you trust"
      else raise ConfigurationError, "unsupported URL scheme #{uri.scheme.inspect}"
      end
    rescue URI::InvalidURIError => e
      raise ConfigurationError, "invalid server URL: #{e.message}"
    end

    # The token is opaque and never parsed; only a value that cannot form a header is refused.
    def validate_token(token)
      raise ConfigurationError, "a token is required" if token.nil? || token.to_s.strip.empty?
      raise ConfigurationError, "a token cannot contain line breaks" if token.to_s.match?(/[\r\n]/)

      token.to_s
    end

    def build_http(read_timeout)
      http = Net::HTTP.new(@uri.host, @uri.port)
      http.open_timeout = @open_timeout
      http.read_timeout = read_timeout
      http.write_timeout = @write_timeout
      configure_tls(http, @tls) if @uri.scheme == "https"
      http
    end

    def configure_tls(http, tls)
      unknown = tls.keys - %i[ca_file ca_path cert key]
      raise ConfigurationError, "unknown tls option(s): #{unknown.join(", ")}" unless unknown.empty?

      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_PEER
      http.verify_hostname = true
      http.min_version = OpenSSL::SSL::TLS1_2_VERSION
      http.ca_file = tls[:ca_file] if tls[:ca_file]
      http.ca_path = tls[:ca_path] if tls[:ca_path]
      http.cert = tls[:cert] if tls[:cert]
      http.key = tls[:key] if tls[:key]
    end

    # A base URL may carry a path prefix, for a server mounted behind a proxy.
    def full_path(path) = "#{@uri.path.chomp("/")}#{path}"

    def with_query(path, query)
      query = query.compact
      query.empty? ? path : "#{path}?#{URI.encode_www_form(query)}"
    end

    def authorize(request, accept)
      request["Authorization"] = @authorization
      request["Accept"] = accept
      request["User-Agent"] = @user_agent
    end

    def read_timeout_until(deadline) = deadline ? deadline.cap(@watch_read_timeout) : @watch_read_timeout

    def perform(request)
      authorize(request, "application/json")
      response = @lock.synchronize do
        @http.start unless @http.started?
        @http.request(request)
      end
      interpret(response)
    rescue *NETWORK_ERRORS => e
      close_quietly
      raise TransportError, "#{request.method} #{request.path}: #{e.class}: #{e.message}"
    end

    def interpret(response)
      status = response.code.to_i
      refuse(response, status) unless status.between?(200, 299)

      success_body(parse_json(response.body), status)
    end

    # A 2xx that is not an event stream came from something else, e.g. a proxy.
    def expect_event_stream(response)
      status = response.code.to_i
      refuse(response, status) unless status.between?(200, 299)
      return if response.content_type == EVENT_STREAM

      raise TransportError, "HTTP #{status} answered #{response.content_type.inspect}, not #{EVENT_STREAM}"
    end

    def refuse(response, status)
      body = parse_json(response.body)
      error = body.is_a?(Hash) && body["error"].is_a?(Hash) ? body["error"] : nil
      raise RPCError.from_status(error, http_status: status) if error

      raise RPCError.from_http(status, response.body)
    end

    def success_body(body, status)
      raise TransportError, "HTTP #{status} answered with a body that is not a JSON object" unless body.is_a?(Hash)

      body
    end

    def parse_json(text)
      return nil if text.nil? || text.empty?

      JSON.parse(text)
    rescue JSON::ParserError
      nil
    end

    # A connection that failed mid-request cannot be reused; the next call reconnects.
    def close_quietly
      @lock.synchronize { @http.finish if @http.started? }
    rescue IOError
      nil
    end
  end
end
