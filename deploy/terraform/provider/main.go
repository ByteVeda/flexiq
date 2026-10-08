// Command terraform-provider-flexiq is the Terraform provider for a
// flexiq-server's admin door.
package main

import (
	"context"
	"flag"
	"log"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"

	"github.com/ByteVeda/flexiq/deploy/terraform/provider/internal/provider"
)

// version is stamped by the release build (-ldflags "-X main.version=...").
var version = "dev"

func main() {
	var debug bool
	flag.BoolVar(&debug, "debug", false, "start the provider for a debugger (delve) to attach to")
	flag.Parse()

	err := providerserver.Serve(context.Background(), provider.New(version), providerserver.ServeOpts{
		Address: provider.RegistryAddress,
		Debug:   debug,
	})
	if err != nil {
		log.Fatal(err)
	}
}
