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
