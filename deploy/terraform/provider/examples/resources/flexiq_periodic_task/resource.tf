# Six cron fields, seconds first: this fires daily at 08:00 Berlin time.
resource "flexiq_periodic_task" "digest" {
  name      = "daily-digest"
  task_name = "reports.send_digest"
  cron      = "0 0 8 * * *"
  timezone  = "Europe/Berlin"
  queue     = "emails"

  args   = jsonencode(["ops"])
  kwargs = jsonencode({ window_hours = 24 })

  enabled = true
}
