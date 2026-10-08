# An operator override on one queue. It reaches workers at their next start.
resource "flexiq_queue" "emails" {
  name           = "emails"
  max_concurrent = 8
  rate_limit     = "100/m"
  paused         = false
}
