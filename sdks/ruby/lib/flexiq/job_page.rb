# frozen_string_literal: true

module FlexiQ
  # One page of `Client#list_jobs`, newest first. Rows never carry payload or result.
  #
  # `next_page_token` is opaque: pass it back as `page_token:` and read nothing out of it.
  # It is nil on the last page.
  JobPage = Data.define(:jobs, :next_page_token) do
    def self.from_json(json)
      jobs = json.fetch("jobs", [])
      raise TransportError, "the server answered a listing whose jobs are not a list" unless jobs.is_a?(Array)

      token = json["nextPageToken"]
      new(jobs: jobs.map { |job| Job.from_json(job) }, next_page_token: token.nil? || token.empty? ? nil : token)
    end

    def last_page? = next_page_token.nil?
  end
end
