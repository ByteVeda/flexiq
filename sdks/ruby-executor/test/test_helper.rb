# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)

require "flexiq/executor"
require "minitest/autorun"

V1 = FlexiQ::Executor::V1

# A job frame as the scheduler sends it; keywords override the defaults.
def job_frame(**fields)
  V1::JobFrame.new(id: "job-1", task_name: "add", payload: FlexiQ::Payload.encode_call([1, 2], { "k" => "v" }),
                   queue: "work", retry_count: 1, max_retries: 3, **fields)
end

# Workers in tests log nowhere; the suite's output is the assertions.
def quiet_logger = Logger.new(File::NULL)
