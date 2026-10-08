terraform {
  required_providers {
    flexiq = {
      source = "registry.terraform.io/byteveda/flexiq"
    }
  }
}

variable "flexiq_token" {
  type      = string
  sensitive = true
}

# The server reads the namespace from the token, so one provider block manages
# one namespace. `namespace` is only the label recorded in state.
provider "flexiq" {
  address   = "flexiq.example.com:443"
  token     = var.flexiq_token
  namespace = "payments"
}

# A second namespace needs a token minted for it and an alias of its own.
provider "flexiq" {
  alias     = "billing"
  address   = "flexiq.example.com:443"
  token     = var.flexiq_billing_token
  namespace = "billing"

  # A server with a private CA. `insecure = true` would send the token in
  # plaintext, for loopback only.
  tls {
    ca_cert = file("${path.module}/ca.pem")
  }
}

variable "flexiq_billing_token" {
  type      = string
  sensitive = true
}
