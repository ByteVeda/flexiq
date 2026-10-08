# terraform-provider-flexiq

Terraform provider for a flexiq-server's gRPC admin door
(`registry.terraform.io/byteveda/flexiq`), built on the Go admin client in
`sdks/go/admin`.

The server reads the namespace from the token, so one provider block manages
one namespace. Use one provider alias per namespace, each with a token minted
for it.

```sh
make check            # build, vet, golangci-lint, unit tests
make server testacc   # acceptance tests against a real flexiq-server
```

## Tokens

`flexiq_token` needs a provider token with the `tokens` scope and grants that
cover every scope it mints. The server refuses a token that would outlive the
caller, so the provider's own token must outlive every token it mints —
replacements included. `expire_days` is therefore required.

`rotate_before_days` plans a replacement that many days before expiry; a
revoked or expired token is replaced on the next plan too. With
`create_before_destroy`, the new secret exists before the old token is revoked:

```hcl
resource "flexiq_token" "worker" {
  name               = "worker"
  scopes             = ["execute"]
  expire_days        = 30
  rotate_before_days = 7

  lifecycle {
    create_before_destroy = true
  }
}

resource "kubernetes_secret" "worker_token" {
  metadata {
    name = "flexiq-worker-token"
  }
  data = {
    FLEXIQ_TOKEN = flexiq_token.worker.secret
  }
}
```

The secret is known only from the apply that minted it. A token imported by its id
has a null `secret`, so rotate an imported token before wiring its secret
anywhere.
