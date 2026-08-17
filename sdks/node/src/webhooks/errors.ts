import { FlexiQError } from "../errors";

/** Thrown when a webhook definition fails validation. Dashboard maps it to 400. */
export class WebhookValidationError extends FlexiQError {
  constructor(message: string) {
    super(message);
    this.name = "WebhookValidationError";
  }
}

/**
 * Thrown when a webhook URL targets a destination we refuse to deliver to
 * (SSRF guard). A validation error so the dashboard still answers 400.
 */
export class UnsafeWebhookUrlError extends WebhookValidationError {
  constructor(message: string) {
    super(message);
    this.name = "UnsafeWebhookUrlError";
  }
}
