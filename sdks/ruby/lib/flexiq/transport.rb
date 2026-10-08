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

    # `tls:` keys: :ca_file, :ca_path, :cert (OpenSSL::X509::Certificate), :key (OpenSSL::PKey).
    def initialize(url, token:, insecure: false, tls: {}, open_timeout: 5, read_timeout: 30,
                   write_timeout: 30, user_agent: nil)
      @uri = parse_url(url, insecure)
      @authorization = "Bearer #{validate_token(token)}".freeze
      @user_agent = [user_agent, "flexiq-ruby/#{VERSION}"].compact.join(" ").freeze
      @http = build_http(tls, open_timeout, read_timeout, write_timeout)
      @lock = Mutex.new
    end

    def get(path, query = {})
      query = query.compact
      target = query.empty? ? path : "#{path}?#{URI.encode_www_form(query)}"
      perform(Net::HTTP::Get.new(full_path(target)))
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

    def build_http(tls, open_timeout, read_timeout, write_timeout)
      http = Net::HTTP.new(@uri.host, @uri.port)
      http.open_timeout = open_timeout
      http.read_timeout = read_timeout
      http.write_timeout = write_timeout
      configure_tls(http, tls) if @uri.scheme == "https"
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

    def perform(request)
      request["Authorization"] = @authorization
      request["Accept"] = "application/json"
      request["User-Agent"] = @user_agent
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
      body = parse_json(response.body)
      return success_body(body, status) if status.between?(200, 299)

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
