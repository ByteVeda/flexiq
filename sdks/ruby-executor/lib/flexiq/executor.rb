# frozen_string_literal: true

# FlexiQ executor client over a flexiq-server's executor door (flexiq.executor.v1).
# The wire contract is contracts/REMOTE_SDK_CONTRACT.md in the FlexiQ repository.
#
# An executor cannot enqueue: the door has no enqueue-shaped RPC. A task that fans out goes back
# through the producer door with the `flexiq` gem and a produce-scoped credential of its own.

require "flexiq"
require "grpc"

require_relative "executor/version"
require_relative "executor/v1/executor_service_services_pb"

require_relative "executor/clock"
require_relative "executor/errors"
require_relative "executor/config"
require_relative "executor/job"
require_relative "executor/outcome"
require_relative "executor/outbox"
require_relative "executor/slots"
require_relative "executor/backoff"
