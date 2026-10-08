package provider

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	tfresource "github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// Acceptance tests: terraform plans and applies against the harness's real
// flexiq-server. tfresource.Test skips them unless TF_ACC is set.

var accFactories = map[string]func() (tfprotov6.ProviderServer, error){
	"flexiq": providerserver.NewProtocol6WithError(New("acc")()),
}

// requireAcc skips without TF_ACC. It runs before a test builds its steps,
// since the configs read the harness's address.
func requireAcc(t *testing.T) {
	t.Helper()
	if os.Getenv("TF_ACC") == "" {
		t.Skip("acceptance test; set TF_ACC=1")
	}
	if accServer == nil {
		t.Fatal("TF_ACC is set but the harness started no flexiq-server")
	}
}

// accConfig is a provider block for the harness's server followed by body.
func accConfig(body string) string {
	return fmt.Sprintf(`
provider "flexiq" {
  address   = %q
  token     = %q
  namespace = %q
  tls {
    insecure = true
  }
}
`, accServer.addr, accServer.token, accNamespace) + body
}

// checkServer runs fn as a check step, failing it with fn's error.
func checkServer(fn func(ctx context.Context) error) tfresource.TestCheckFunc {
	return func(*terraform.State) error { return fn(context.Background()) }
}

// accQueue is the queue TestAccQueue manages.
const accQueue = "tf-acc-emails"

// expectQueue checks accQueue's override and pause state on the server.
func expectQueue(maxConcurrent *int32, rate string, paused bool) tfresource.TestCheckFunc {
	return checkServer(func(ctx context.Context) error {
		override, _, err := accServer.client.GetQueueOverride(ctx, accQueue)
		if err != nil {
			return err
		}
		queue, _, err := accServer.client.GetQueue(ctx, accQueue)
		if err != nil {
			return err
		}
		switch {
		case (override.MaxConcurrent == nil) != (maxConcurrent == nil),
			maxConcurrent != nil && *override.MaxConcurrent != *maxConcurrent:
			return fmt.Errorf("max_concurrent on the server = %v, want %v", override.MaxConcurrent, maxConcurrent)
		case override.RateLimit != rate:
			return fmt.Errorf("rate_limit on the server = %q, want %q", override.RateLimit, rate)
		case queue.Paused != paused:
			return fmt.Errorf("paused on the server = %v, want %v", queue.Paused, paused)
		}
		return nil
	})
}

func TestAccQueue(t *testing.T) {
	const address = "flexiq_queue.emails"
	requireAcc(t)

	tfresource.Test(t, tfresource.TestCase{
		ProtoV6ProviderFactories: accFactories,
		CheckDestroy:             expectQueue(nil, "", false),
		Steps: []tfresource.TestStep{
			{
				Config: accConfig(`
resource "flexiq_queue" "emails" {
  name           = "tf-acc-emails"
  max_concurrent = 0
  rate_limit     = "100/m"
  paused         = true
}
`),
				Check: tfresource.ComposeAggregateTestCheckFunc(
					tfresource.TestCheckResourceAttr(address, "id", accQueue),
					tfresource.TestCheckResourceAttr(address, "max_concurrent", "0"),
					tfresource.TestCheckResourceAttr(address, "rate_limit", "100/m"),
					tfresource.TestCheckResourceAttr(address, "paused", "true"),
					expectQueue(int32Ptr(0), "100/m", true),
				),
			},
			{
				Config: accConfig(`
resource "flexiq_queue" "emails" {
  name           = "tf-acc-emails"
  max_concurrent = 4
}
`),
				Check: tfresource.ComposeAggregateTestCheckFunc(
					tfresource.TestCheckResourceAttr(address, "max_concurrent", "4"),
					tfresource.TestCheckNoResourceAttr(address, "rate_limit"),
					tfresource.TestCheckResourceAttr(address, "paused", "false"),
					expectQueue(int32Ptr(4), "", false),
				),
			},
			{
				// Drift: the override is cleared and the queue paused behind
				// terraform's back; the same config puts both back.
				PreConfig: func() {
					ctx := context.Background()
					if err := accServer.client.ClearQueueOverride(ctx, accQueue); err != nil {
						t.Fatalf("clear out of band: %v", err)
					}
					if _, err := accServer.client.PauseQueue(ctx, accQueue); err != nil {
						t.Fatalf("pause out of band: %v", err)
					}
				},
				Config: accConfig(`
resource "flexiq_queue" "emails" {
  name           = "tf-acc-emails"
  max_concurrent = 4
}
`),
				Check: expectQueue(int32Ptr(4), "", false),
			},
			{
				ResourceName:      address,
				ImportState:       true,
				ImportStateId:     accQueue,
				ImportStateVerify: true,
			},
		},
	})
}

func TestAccQueueRejectsABadRateAtPlan(t *testing.T) {
	requireAcc(t)
	tfresource.Test(t, tfresource.TestCase{
		ProtoV6ProviderFactories: accFactories,
		Steps: []tfresource.TestStep{{
			Config: accConfig(`
resource "flexiq_queue" "bad" {
  name       = "tf-acc-bad"
  rate_limit = "0/s"
}
`),
			PlanOnly:    true,
			ExpectError: regexp.MustCompile(`count must be at least one`),
		}},
	})
}

func expectQuota(check func(maxPending, maxRunning, maxDeadRows *int64, rate, onExcess string) error) tfresource.TestCheckFunc {
	return checkServer(func(ctx context.Context) error {
		q, err := accServer.client.GetNamespaceQuota(ctx)
		if err != nil {
			return err
		}
		return check(q.MaxPending, q.MaxRunning, q.MaxDeadRows, q.EnqueueRate, q.OnExcess.String())
	})
}

func TestAccNamespace(t *testing.T) {
	const address = "flexiq_namespace.this"
	requireAcc(t)

	tfresource.Test(t, tfresource.TestCase{
		ProtoV6ProviderFactories: accFactories,
		CheckDestroy: expectQuota(func(maxPending, maxRunning, maxDeadRows *int64, rate, _ string) error {
			if maxPending != nil || maxRunning != nil || maxDeadRows != nil || rate != "" {
				return fmt.Errorf("quota left after destroy: %v %v %v %q", maxPending, maxRunning, maxDeadRows, rate)
			}
			return nil
		}),
		Steps: []tfresource.TestStep{
			{
				Config: accConfig(`
resource "flexiq_namespace" "this" {
  max_pending   = 1000
  on_excess     = "drop"
  enqueue_rate  = "100/s"
  max_dead_rows = 0
}
`),
				Check: tfresource.ComposeAggregateTestCheckFunc(
					tfresource.TestCheckResourceAttr(address, "id", accNamespace),
					tfresource.TestCheckResourceAttr(address, "name", accNamespace),
					tfresource.TestCheckResourceAttr(address, "max_dead_rows", "0"),
					tfresource.TestCheckNoResourceAttr(address, "max_running"),
					expectQuota(func(maxPending, maxRunning, maxDeadRows *int64, rate, onExcess string) error {
						if maxPending == nil || *maxPending != 1000 || maxRunning != nil ||
							maxDeadRows == nil || *maxDeadRows != 0 || rate != "100/s" || onExcess != "DROP" {
							return fmt.Errorf("server quota: %v %v %v %q %s", maxPending, maxRunning, maxDeadRows, rate, onExcess)
						}
						return nil
					}),
				),
			},
			{
				Config: accConfig(`
resource "flexiq_namespace" "this" {
  max_running = 5
}
`),
				Check: tfresource.ComposeAggregateTestCheckFunc(
					tfresource.TestCheckResourceAttr(address, "on_excess", "reject"),
					tfresource.TestCheckNoResourceAttr(address, "max_pending"),
					expectQuota(func(maxPending, maxRunning, _ *int64, rate, onExcess string) error {
						if maxPending != nil || maxRunning == nil || *maxRunning != 5 || rate != "" || onExcess != "REJECT" {
							return fmt.Errorf("server quota: %v %v %q %s", maxPending, maxRunning, rate, onExcess)
						}
						return nil
					}),
				),
			},
			{
				// Drift: the quota is cleared behind terraform's back.
				PreConfig: func() {
					if err := accServer.client.ClearNamespaceQuota(context.Background()); err != nil {
						t.Fatalf("clear out of band: %v", err)
					}
				},
				Config: accConfig(`
resource "flexiq_namespace" "this" {
  max_running = 5
}
`),
				Check: expectQuota(func(_, maxRunning, _ *int64, _, _ string) error {
					if maxRunning == nil || *maxRunning != 5 {
						return fmt.Errorf("max_running not restored: %v", maxRunning)
					}
					return nil
				}),
			},
			{
				ResourceName:      address,
				ImportState:       true,
				ImportStateId:     accNamespace,
				ImportStateVerify: true,
			},
		},
	})
}
