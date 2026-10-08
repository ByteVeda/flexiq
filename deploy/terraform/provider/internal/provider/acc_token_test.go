package provider

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"testing"

	tfresource "github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/hashicorp/terraform-plugin-testing/tfjsonpath"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

const accTokenAddress = "flexiq_token.reader"

func tokenConfig(rotateBeforeDays int) string {
	return accConfig(fmt.Sprintf(`
resource "flexiq_token" "reader" {
  name               = "tf-acc-reader"
  scopes             = ["inspect"]
  expire_days        = 30
  rotate_before_days = %d

  lifecycle {
    create_before_destroy = true
  }
}
`, rotateBeforeDays))
}

// tokenAttrs reads the token's id and secret from terraform's state.
func tokenAttrs(s *terraform.State) (id, secret string, err error) {
	rs, ok := s.RootModule().Resources[accTokenAddress]
	if !ok {
		return "", "", fmt.Errorf("%s not in state", accTokenAddress)
	}
	return rs.Primary.ID, rs.Primary.Attributes["secret"], nil
}

// asToken runs fn with an admin client that presents secret.
func asToken(secret string, fn func(*admin.Client) error) error {
	client, err := admin.New(accServer.addr, flexiq.WithToken(secret), flexiq.WithInsecureTransport())
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	return fn(client)
}

// expectWorkingSecret proves the minted secret opens what its inspect grant
// covers, and nothing beyond it. It records the token's id in *id.
func expectWorkingSecret(id *string) tfresource.TestCheckFunc {
	return func(s *terraform.State) error {
		tokenID, secret, err := tokenAttrs(s)
		if err != nil {
			return err
		}
		*id = tokenID
		ctx := context.Background()
		return asToken(secret, func(client *admin.Client) error {
			if _, err := client.ListQueues(ctx); err != nil {
				return fmt.Errorf("the minted secret cannot list queues: %w", err)
			}
			if _, err := client.PauseQueue(ctx, "tf-acc-never"); !errors.Is(err, flexiq.ReasonScopeDenied) {
				return fmt.Errorf("an inspect token paused a queue: err = %w, want SCOPE_DENIED", err)
			}
			return nil
		})
	}
}

// expectRevoked checks the token with *id is revoked on the server.
func expectRevoked(id *string) tfresource.TestCheckFunc {
	return checkServer(func(ctx context.Context) error {
		token, err := accServer.client.GetToken(ctx, *id)
		if err != nil {
			return err
		}
		if token.Status != admin.TokenStatusRevoked {
			return fmt.Errorf("token %s is %s, want REVOKED", *id, token.Status)
		}
		return nil
	})
}

// expectReplaced checks the token in state is not the one with *old.
func expectReplaced(old *string) tfresource.TestCheckFunc {
	return func(s *terraform.State) error {
		id, _, err := tokenAttrs(s)
		if err != nil {
			return err
		}
		if id == *old {
			return fmt.Errorf("token %s was not replaced", id)
		}
		return nil
	}
}

func TestAccToken(t *testing.T) {
	requireAcc(t)
	var first, second string
	replaceFirst := []plancheck.PlanCheck{
		plancheck.ExpectResourceAction(accTokenAddress, plancheck.ResourceActionCreateBeforeDestroy),
	}

	tfresource.Test(t, tfresource.TestCase{
		ProtoV6ProviderFactories: accFactories,
		CheckDestroy:             expectRevoked(&second),
		Steps: []tfresource.TestStep{
			{
				Config: tokenConfig(0),
				ConfigPlanChecks: tfresource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectSensitiveValue(accTokenAddress, tfjsonpath.New("secret"))},
				},
				Check: tfresource.ComposeAggregateTestCheckFunc(
					tfresource.TestCheckResourceAttrSet(accTokenAddress, "secret"),
					tfresource.TestCheckResourceAttr(accTokenAddress, "status", "ACTIVE"),
					tfresource.TestCheckResourceAttr(accTokenAddress, "namespace", accNamespace),
					expectWorkingSecret(&first),
				),
			},
			{
				// The secret is unrecoverable: an import holds null.
				ResourceName:            accTokenAddress,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"secret"},
				ImportStateCheck: func(states []*terraform.InstanceState) error {
					if len(states) != 1 {
						return fmt.Errorf("imported %d instances, want 1", len(states))
					}
					if secret, ok := states[0].Attributes["secret"]; ok && secret != "" {
						return errors.New("an imported token carries a secret")
					}
					return nil
				},
			},
			{
				// Revoked out of band: the next plan replaces it.
				PreConfig: func() {
					if _, err := accServer.client.RevokeToken(context.Background(), first); err != nil {
						t.Fatalf("revoke out of band: %v", err)
					}
				},
				Config:           tokenConfig(0),
				ConfigPlanChecks: tfresource.ConfigPlanChecks{PreApply: replaceFirst},
				Check: tfresource.ComposeAggregateTestCheckFunc(
					expectReplaced(&first),
					expectWorkingSecret(&second),
				),
			},
		},
	})
}

// TestAccTokenRejectsALoopingWindow: a window as long as the lifetime would
// make every new token due at once and every plan replace it, so it is refused
// at plan time. The window itself is covered by TestTokenModifyPlan's clock.
func TestAccTokenRejectsALoopingWindow(t *testing.T) {
	requireAcc(t)
	tfresource.Test(t, tfresource.TestCase{
		ProtoV6ProviderFactories: accFactories,
		Steps: []tfresource.TestStep{{
			Config:      tokenConfig(30),
			PlanOnly:    true,
			ExpectError: regexp.MustCompile(`must\s+be\s+less\s+than\s+expire_days`),
		}},
	})
}
