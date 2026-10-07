# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)

require "flexiq"
require "minitest/autorun"

# Repository root, for the shared contract files under contracts/.
REPO_ROOT = File.expand_path("../../..", __dir__)

def hex(bytes) = bytes.unpack1("H*")

def unhex(text) = [text].pack("H*")
