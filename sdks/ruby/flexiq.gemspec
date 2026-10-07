# frozen_string_literal: true

require_relative "lib/flexiq/version"

Gem::Specification.new do |spec|
  spec.name = "flexiq"
  spec.version = FlexiQ::VERSION
  spec.authors = ["ByteVeda"]
  spec.summary = "Ruby client for the FlexiQ producer door"
  spec.description = <<~DESC
    Submit, read, cancel and count FlexiQ jobs over a running flexiq-server's
    JSON door. Pure Ruby, standard library only: no database credential, no
    native extension, no gRPC toolchain.
  DESC
  spec.homepage = "https://github.com/ByteVeda/flexiq"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.3"

  spec.metadata = {
    "homepage_uri" => spec.homepage,
    "source_code_uri" => "https://github.com/ByteVeda/flexiq/tree/master/sdks/ruby",
    "documentation_uri" => "https://docs.byteveda.org/flexiq/server/clients",
    "bug_tracker_uri" => "https://github.com/ByteVeda/flexiq/issues",
    "rubygems_mfa_required" => "true"
  }

  spec.files = Dir["lib/**/*.rb", "README.md"]
  spec.require_paths = ["lib"]
end
