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
