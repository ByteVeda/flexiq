# frozen_string_literal: true

module FlexiQ
  # A producer client: submit work and workflows, read them back, list, cancel and count jobs.
  #
  #   client = FlexiQ::Client.new("https://flexiq.internal:50051", token: ENV.fetch("FLEXIQ_TOKEN"))
  #   job = client.enqueue("send_receipt", args: [{ "order_id" => "o-1" }], queue: "emails").job
  #   client.get_job(job.id).status # => :pending
  #
  # The namespace is the token's, fixed when it was minted; no call can name another.
  # Methods raise RPCError when the server refuses and TransportError when no answer arrived.
  # Thread-safe; calls on one client are serialised over one connection, and each watch holds a
  # connection of its own.
  class Client
    # See Transport#initialize for the keyword options (TLS, timeouts, user agent).
    def initialize(url, token:, transport: nil, **)
      @transport = transport || Transport.new(url, token: token, **)
    end

    # Submits one job. Keywords other than `args:` / `kwargs:` are EnqueueOptions.
    #
    # Not idempotent. If this raises TransportError, or RPCError with code UNAVAILABLE,
    # DEADLINE_EXCEEDED or CANCELLED, the job may exist; resend only with `unique_key` set.
    def enqueue(task_name, args: [], kwargs: {}, **options)
      request = EnqueueRequest.new(task_name: task_name, args: args, kwargs: kwargs, options: options)
      EnqueueResult.from_json(@transport.post("/v1/jobs", request.to_wire))
    end

    # Submits several jobs in one call; returns one BatchItemResult per request, in order.
    #
    # Not atomic. An item's error is that item alone. When the whole call raises instead,
    # nothing was enqueued and `RPCError#batch_index` names the item that failed.
    def enqueue_batch(requests)
      items = requests.map { |request| coerce_request(request).to_wire }
      response = @transport.post("/v1/jobs:batchEnqueue", { "items" => items })
      batch_results(response["results"], items.length)
    end

    # Reads one job. The payload and result are left out unless asked for: they are the largest
    # things a job carries. A job in another namespace reads as JOB_NOT_FOUND.
    def get_job(job_id, include_payload: false, include_result: false)
      fetch_job(job_id, include_payload: include_payload, include_result: include_result)
    end

    # Reads one page of jobs, newest first. Every filter is optional; `status` is a
    # FlexiQ::JobStatus symbol. A grant narrowed to queues needs a `queue:` it reaches, one
    # narrowed to tasks a `task_name:`, or the call raises RPCError SCOPE_DENIED: a listing is
    # refused, never filtered silently.
    def list_jobs(status: nil, queue: nil, task_name: nil, page_size: nil, page_token: nil)
      query = {
        "status" => status.nil? ? nil : JobStatus.dump(status), "queue" => queue, "taskName" => task_name,
        "pageSize" => page_size, "pageToken" => page_token
      }
      JobPage.from_json(@transport.get("/v1/jobs", query))
    end

    # Every job `list_jobs` reaches with these filters, one page at a time. Without a block,
    # returns an Enumerator; a page is fetched only when iteration reaches it.
    def each_job(status: nil, queue: nil, task_name: nil, page_size: nil, &block)
      return enum_for(:each_job, status: status, queue: queue, task_name: task_name, page_size: page_size) unless block

      filters = { status: status, queue: queue, task_name: task_name, page_size: page_size }

      # Not `loop`: it would swallow a StopIteration the caller's block raised.
      page = nil
      until page&.last_page?
        page = list_jobs(**filters, page_token: page&.next_page_token)
        page.jobs.each(&block)
      end
    end

    # Requests cancellation and returns the job as the call left it. Idempotent.
    def cancel_job(job_id)
      job_from(@transport.post("/v1/jobs/#{Wire::Path.segment(job_id)}:cancel"))
    end

    # Counts jobs by state in `queue`, or across the namespace when `queue` is nil.
    def queue_stats(queue = nil)
      path = queue.nil? ? "/v1/stats" : "/v1/queues/#{Wire::Path.segment(queue)}/stats"
      QueueStats.from_json(@transport.get(path))
    end

    # Follows jobs by id until each is `terminal?`. Yields a `:snapshot` JobTransition (or
    # JobNotFound) per id, then live transitions. At most 100 ids. Enumerator without a block.
    #
    # A dropped stream reopens on the unfinished ids only.
    def watch_jobs(job_ids, &block)
      return enum_for(:watch_jobs, job_ids) unless block

      Watch::ByIds.new(@transport, job_ids).run(&block)
      nil
    end

    # Follows `queue` (nil = "default") until the block breaks. Yields a WatchCheckpoint, then
    # live JobTransitions; no snapshot. Pass the last `cursor` back as `resume_cursor:` to replay.
    #
    # A dropped stream resumes from its last cursor; an expired one yields a WatchGap.
    # Sees only transitions the reached server process handles.
    def watch_queue(queue = nil, resume_cursor: nil, &block)
      return enum_for(:watch_queue, queue, resume_cursor: resume_cursor) unless block

      Watch::ByQueue.new(@transport, queue, resume_cursor).run(&block)
    end

    # Blocks until the job is finished; returns it with its result. Watches, never polls.
    # Raises WaitTimeoutError after `timeout` seconds (nil: no limit), RPCError JOB_NOT_FOUND
    # for an id this token cannot see.
    def wait(job_id, timeout:)
      deadline = Deadline.within(timeout)
      Watch::ByIds.new(@transport, [job_id], deadline: deadline).run { nil }
      read_finished(job_id, deadline)
    end

    # Submits a static workflow graph (a FlexiQ::WorkflowGraph) and returns the new run's id.
    # The server enqueues one job per node, chained by the edges; any worker with workflow
    # tracking enabled advances the run. `params_json` is JSON text the run carries, never a call.
    #
    # Static graphs only: a node setting gate, cache, fan_out, fan_in or sub_workflow is refused
    # with WORKFLOW_CONSTRUCT_UNSUPPORTED, naming one node (`RPCError#workflow_construct`).
    # Every submission is version 1 of `name`; a different graph under a used name is refused.
    #
    # Not idempotent, and there is no `unique_key`: a run cannot be found by name, so a call that
    # raised TransportError, or RPCError UNAVAILABLE / DEADLINE_EXCEEDED / CANCELLED, may have
    # submitted a run whose id is lost. A retry submits a second run; never retry blind.
    def submit_workflow(name, graph, params_json: nil)
      raise ArgumentError, "a workflow name must be a non-empty String" unless name.is_a?(String) && !name.empty?
      raise ArgumentError, "graph must be a FlexiQ::WorkflowGraph, got #{graph.class}" unless graph.is_a?(WorkflowGraph)
      unless params_json.nil? || params_json.is_a?(String)
        raise ArgumentError, "params_json must be JSON text (a String), got #{params_json.class}"
      end

      body = { "name" => name, "graph" => graph.validate!.to_wire, "paramsJson" => params_json }.compact
      run_id = @transport.post("/v1/workflows", body)["runId"]
      raise TransportError, "the server answered a submission without a run id" if run_id.to_s.empty?

      run_id
    end

    # Reads a workflow run and every node it has, as a WorkflowRunView. A run in another
    # namespace, or one whose jobs a narrowed grant does not all reach, reads as NOT_FOUND.
    def get_workflow_run(run_id)
      WorkflowRunView.from_json(@transport.get("/v1/workflows/#{Wire::Path.segment(run_id)}"))
    end

    # Closes the underlying connection; the next call reopens it.
    def close = @transport.close

    private

    # The job is done, so only the read can fail; retry what clears, within the deadline.
    # The deadline also caps this read, so a stalled answer cannot outlast `wait`'s timeout.
    def read_finished(job_id, deadline)
      backoff = Backoff.new
      begin
        fetch_job(job_id, include_result: true, deadline: deadline)
      rescue TransportError, RPCError => e
        raise if e.is_a?(RPCError) && !e.retryable?
        raise WaitTimeoutError, "gave up after #{deadline.seconds}s reading job #{job_id}" if deadline&.expired?

        backoff.pause(deadline)
        retry
      end
    end

    def fetch_job(job_id, include_payload: false, include_result: false, deadline: nil)
      query = { "includePayload" => include_payload || nil, "includeResult" => include_result || nil }
      job_from(@transport.get("/v1/jobs/#{Wire::Path.segment(job_id)}", query, deadline: deadline))
    end

    def coerce_request(request)
      case request
      when EnqueueRequest then request
      when Hash then EnqueueRequest.new(**request)
      else raise ArgumentError, "a batch item must be an EnqueueRequest or a Hash, got #{request.class}"
      end
    end

    # A short or long answer cannot be paired with its requests; guessing would attribute an
    # outcome to the wrong job, or report an item that never landed as handled.
    def batch_results(results, expected)
      unless results.is_a?(Array) && results.length == expected
        got = results.is_a?(Array) ? results.length : "no"
        raise TransportError, "the batch answered #{got} result(s) for #{expected} item(s)"
      end

      results.each_with_index.map { |result, index| BatchItemResult.from_json(result, index) }
    end

    def job_from(response)
      json = response["job"]
      raise TransportError, "the server answered without a job" unless json.is_a?(Hash)

      Job.from_json(json)
    end
  end
end
