# Needs a provider token with the `tokens` scope and grants covering `execute`.
# The provider's own token must outlive every token minted here.
resource "flexiq_token" "worker" {
  name               = "worker"
  scopes             = ["execute"]
  expire_days        = 30
  rotate_before_days = 7

  # The new secret exists before the old token is revoked.
  lifecycle {
    create_before_destroy = true
  }
}

output "worker_token" {
  value     = flexiq_token.worker.secret
  sensitive = true
}
