# The quota of the namespace the provider's token belongs to. An unset limit is
# unlimited; 0 is a real limit.
resource "flexiq_namespace" "payments" {
  max_pending       = 10000
  max_running       = 50
  enqueue_rate      = "500/s"
  on_excess         = "drop"
  max_archived_rows = 1000000
  max_dead_rows     = 50000
}
