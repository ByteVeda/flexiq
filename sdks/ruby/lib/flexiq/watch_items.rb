# frozen_string_literal: true

module FlexiQ
  # An id this token cannot see (missing or another namespace). Finished.
  JobNotFound = Data.define(:job_id)

  # A queue-watch position with nothing to report: the opening checkpoint or an unknown arm.
  # Keep its cursor like any other.
  WatchCheckpoint = Data.define(:cursor)

  # The resume cursor expired; transitions after `lost_cursor` were missed.
  WatchGap = Data.define(:lost_cursor)
end
