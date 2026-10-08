# frozen_string_literal: true

require_relative "lib/flexiq/executor/version"

Gem::Specification.new do |spec|
  spec.name = "flexiq-executor"
  spec.version = FlexiQ::Executor::VERSION
  spec.authors = ["ByteVeda"]
  spec.summary = "Ruby client for the FlexiQ executor door"
  spec.description = <<~DESC
    Attach to a running flexiq-server's executor door over gRPC, run the jobs it
    dispatches and settle each one. A separate gem from `flexiq` because this
    door needs a real gRPC library, and the producer gem has no dependencies.
  DESC
  spec.homepage = "https://github.com/ByteVeda/flexiq"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.3"

  spec.metadata = {
    "homepage_uri" => spec.homepage,
    "source_code_uri" => "https://github.com/ByteVeda/flexiq/tree/master/sdks/ruby-executor",
    "documentation_uri" => "https://docs.byteveda.org/flexiq/server/clients",
    "bug_tracker_uri" => "https://github.com/ByteVeda/flexiq/issues",
    "rubygems_mfa_required" => "true"
  }

  # LICENSE is a copy of the repository root's: a gem can only ship files under its own directory.
  spec.files = Dir["lib/**/*.rb", "README.md", "LICENSE"]
  spec.require_paths = ["lib"]

  # The payload codec and the task-error JSON come from the producer gem, at the same release:
  # both halves must agree on the bytes.
  spec.add_dependency "flexiq", FlexiQ::Executor::VERSION
  spec.add_dependency "google-protobuf", "~> 4.36"
  spec.add_dependency "grpc", "~> 1.84"
  # A bundled gem rather than a default one from Ruby 3.5, so it must be declared.
  spec.add_dependency "logger", "~> 1.6"
end
