package provider

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	tfresource "github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

// accPeriodic is the periodic task TestAccPeriodicTask manages.
const accPeriodic = "tf-acc-report"

// expectPeriodic checks accPeriodic on the server, payload decoded.
func expectPeriodic(cron, queue string, enabled bool, args []any, kwargs map[string]any) tfresource.TestCheckFunc {
	return checkServer(func(ctx context.Context) error {
		task, err := accServer.client.GetPeriodicTask(ctx, accPeriodic, admin.GetPeriodicTaskOptions{IncludePayload: true})
		if err != nil {
			return err
		}
		call, err := task.DecodePayload()
		if err != nil {
			return err
		}
		switch {
		case task.Cron != cron:
			return fmt.Errorf("cron on the server = %q, want %q", task.Cron, cron)
		case task.Queue != queue:
			return fmt.Errorf("queue on the server = %q, want %q", task.Queue, queue)
		case task.Enabled != enabled:
			return fmt.Errorf("enabled on the server = %v, want %v", task.Enabled, enabled)
		case !reflect.DeepEqual(call.Args, args):
			return fmt.Errorf("args on the server = %#v, want %#v", call.Args, args)
		case !reflect.DeepEqual(call.Kwargs, kwargs):
			return fmt.Errorf("kwargs on the server = %#v, want %#v", call.Kwargs, kwargs)
		}
		return nil
	})
}

func periodicConfig(body string) string {
	return accConfig(`
resource "flexiq_periodic_task" "report" {
  name      = "tf-acc-report"
  task_name = "send_report"
` + body + `
}
`)
}

func TestAccPeriodicTask(t *testing.T) {
	const address = "flexiq_periodic_task.report"
	requireAcc(t)

	// The config's own spelling: whitespace and a float-looking integer.
	created := periodicConfig(`
  cron   = "0 0 * * * *"
  args   = "[ 1.0, \"a\" ]"
  kwargs = jsonencode({ to = "ops", n = 2 })
`)
	paused := periodicConfig(`
  cron     = "0 */5 * * * *"
  queue    = ""
  timezone = "Europe/Berlin"
  args     = jsonencode([2, 2.5])
  enabled  = false
`)
	final := periodicConfig(`
  cron     = "0 */5 * * * *"
  queue    = "reports"
  timezone = "Europe/Berlin"
  args     = jsonencode([2, 2.5])
`)

	tfresource.Test(t, tfresource.TestCase{
		ProtoV6ProviderFactories: accFactories,
		CheckDestroy: checkServer(func(ctx context.Context) error {
			_, err := accServer.client.GetPeriodicTask(ctx, accPeriodic, admin.GetPeriodicTaskOptions{})
			if !errors.Is(err, flexiq.ReasonPeriodicTaskNotFound) {
				return fmt.Errorf("periodic task after destroy: err = %w, want not found", err)
			}
			return nil
		}),
		Steps: []tfresource.TestStep{
			{
				// Integers go out as CBOR integers (uint64 on decode), never floats.
				Config: created,
				Check: tfresource.ComposeAggregateTestCheckFunc(
					tfresource.TestCheckResourceAttr(address, "id", accPeriodic),
					tfresource.TestCheckResourceAttr(address, "args", "[ 1.0, \"a\" ]"),
					tfresource.TestCheckResourceAttr(address, "queue", defaultQueue),
					tfresource.TestCheckResourceAttr(address, "enabled", "true"),
					expectPeriodic("0 0 * * * *", defaultQueue, true,
						[]any{uint64(1), "a"}, map[string]any{"n": uint64(2), "to": "ops"}),
				),
			},
			{
				// Update cron, args and timezone and disable it. "" means
				// "default", so the state keeps the "default" it already had.
				Config: paused,
				Check: tfresource.ComposeAggregateTestCheckFunc(
					tfresource.TestCheckResourceAttr(address, "queue", defaultQueue),
					tfresource.TestCheckResourceAttr(address, "enabled", "false"),
					tfresource.TestCheckResourceAttr(address, "timezone", "Europe/Berlin"),
					expectPeriodic("0 */5 * * * *", defaultQueue, false,
						[]any{uint64(2), 2.5}, map[string]any{}),
				),
			},
			{
				// The same values spelled differently plan nothing.
				Config: periodicConfig(`
  cron     = "0 */5 * * * *"
  queue    = "default"
  timezone = "Europe/Berlin"
  args     = "[2.0,  2.5]"
  kwargs   = "{ }"
  enabled  = false
`),
				PlanOnly: true,
			},
			{
				// Enable it and move it to a real queue.
				Config: final,
				Check: expectPeriodic("0 */5 * * * *", "reports", true,
					[]any{uint64(2), 2.5}, map[string]any{}),
			},
			{
				// Drift: paused out of band, the same config resumes it.
				PreConfig: func() {
					if _, err := accServer.client.PausePeriodicTask(context.Background(), accPeriodic); err != nil {
						t.Fatalf("pause out of band: %v", err)
					}
				},
				Config: final,
				ConfigPlanChecks: tfresource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectResourceAction(address, plancheck.ResourceActionUpdate)},
				},
				Check: expectPeriodic("0 */5 * * * *", "reports", true,
					[]any{uint64(2), 2.5}, map[string]any{}),
			},
			{
				// Drift: deleted out of band, the same config recreates it.
				PreConfig: func() {
					if err := accServer.client.DeletePeriodicTask(context.Background(), accPeriodic); err != nil {
						t.Fatalf("delete out of band: %v", err)
					}
				},
				Config: final,
				ConfigPlanChecks: tfresource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectResourceAction(address, plancheck.ResourceActionCreate)},
				},
				Check: expectPeriodic("0 */5 * * * *", "reports", true,
					[]any{uint64(2), 2.5}, map[string]any{}),
			},
			{
				ResourceName:      address,
				ImportState:       true,
				ImportStateId:     accPeriodic,
				ImportStateVerify: true,
			},
		},
	})
}
