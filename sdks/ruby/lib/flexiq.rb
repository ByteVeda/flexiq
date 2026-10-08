# frozen_string_literal: true

# FlexiQ producer client over a flexiq-server's JSON door.
# The wire contract is contracts/REMOTE_SDK_CONTRACT.md in the FlexiQ repository.

require_relative "flexiq/version"
require_relative "flexiq/errors"
require_relative "flexiq/deadline"

require_relative "flexiq/cbor/encoder"
require_relative "flexiq/cbor/decoder"
require_relative "flexiq/payload"

require_relative "flexiq/wire/bytes"
require_relative "flexiq/wire/duration"
require_relative "flexiq/wire/int64"
require_relative "flexiq/wire/path"
require_relative "flexiq/wire/sse"
require_relative "flexiq/wire/timestamp"

require_relative "flexiq/reason"
require_relative "flexiq/rpc_error"

require_relative "flexiq/job_status"
require_relative "flexiq/task_error"
require_relative "flexiq/job"
require_relative "flexiq/job_page"
require_relative "flexiq/debounce"
require_relative "flexiq/enqueue_options"
require_relative "flexiq/enqueue_request"
require_relative "flexiq/enqueue_result"
require_relative "flexiq/batch_item_result"
require_relative "flexiq/queue_stats"

require_relative "flexiq/transport"
require_relative "flexiq/client"
