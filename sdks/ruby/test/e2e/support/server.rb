# frozen_string_literal: true

require "fileutils"
require "open3"
require "tmpdir"

# Drives a real flexiq-server for the live suite: a fresh SQLite file per run, a loopback port
# the server picks, tokens minted through its own CLI.
#
# The binary comes from FLEXIQ_SERVER_BIN, else target/{debug,release}/flexiq-server.
class LiveServer
  BINARY_VAR = "FLEXIQ_SERVER_BIN"
  BUILD_COMMAND = "cargo build -p flexiq-server --features grpc"
  NAMESPACE = "ruby-e2e"
  # A queue no worker polls, so enqueued jobs stay pending and observable.
  UNPOLLED_QUEUE = "unpolled"
  BOUND_MARKER = "gRPC listener on tcp://"
  READY_BUDGET = 60
  STOP_GRACE = 10

  attr_reader :url

  def self.binary
    path = ENV.fetch(BINARY_VAR, nil)
    if path
      raise "#{BINARY_VAR}=#{path} does not exist; build one with:\n  #{BUILD_COMMAND}" unless File.executable?(path)

      return path
    end

    root = File.expand_path("../../../../..", __dir__)
    candidates = %w[debug release].map { |profile| File.join(root, "target", profile, "flexiq-server") }
    candidates.find { |candidate| File.executable?(candidate) }
  end

  def initialize(binary)
    @binary = binary
    @dir = Dir.mktmpdir("flexiq-ruby-e2e")
    @dsn = File.join(@dir, "flexiq.db")
    @log = Queue.new
  end

  def start
    stdin, stderr, @wait = Open3.popen2e(env("FLEXIQ_GRPC_LISTEN" => "127.0.0.1:0",
                                             "FLEXIQ_QUEUES" => UNPOLLED_QUEUE,
                                             "FLEXIQ_MAINTENANCE" => "off"), @binary)
    stdin.close
    @url = "http://#{await_address(stderr)}"
    self
  end

  # Mints a token through the server's CLI; prints `fqt_<id>.<secret>` on stdout.
  def mint(name, *scopes)
    args = ["token", "create", "--name", name] + scopes.flat_map { |scope| ["--scope", scope] }
    out, err, status = Open3.capture3(env, @binary, *args)
    raise "flexiq-server #{args.join(" ")} failed:\n#{err}" unless status.success?

    out.strip
  end

  # Bound is not serving: poll a cheap read until the door answers.
  def await_ready(client)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + READY_BUDGET
    begin
      client.queue_stats
    rescue FlexiQ::TransportError, FlexiQ::RPCError => e
      raise "server not ready within #{READY_BUDGET}s (#{e.message}):\n#{log_tail}" if
        Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep 0.1
      retry
    end
  end

  def stop
    return unless @wait

    Process.kill("TERM", @wait.pid)
    @wait.join(STOP_GRACE) || Process.kill("KILL", @wait.pid)
  rescue Errno::ESRCH
    nil
  ensure
    FileUtils.remove_entry(@dir)
  end

  def log_tail = "--- flexiq-server ---\n#{Array.new(@log.size) { @log.pop }.last(40).join}"

  private

  # Inherited FLEXIQ_* settings would point the server at someone else's database.
  def env(extra = {})
    cleared = ENV.keys.select { |name| name.start_with?("FLEXIQ_") || name == "RUST_LOG" }.to_h { [_1, nil] }
    cleared.merge("RUST_LOG" => "info", "FLEXIQ_DSN" => @dsn, "FLEXIQ_NAMESPACE" => NAMESPACE).merge(extra)
  end

  def await_address(stream)
    found = Queue.new
    Thread.new do
      stream.each_line do |line|
        @log << line
        found << line.split(BOUND_MARKER, 2).last.strip if line.include?(BOUND_MARKER)
      end
      found << nil
    end
    address = found.pop(timeout: READY_BUDGET)
    raise "no #{BOUND_MARKER.inspect} line within #{READY_BUDGET}s:\n#{log_tail}" if address.nil?

    address
  end
end
