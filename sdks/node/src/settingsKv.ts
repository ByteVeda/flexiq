// Read-modify-write over the settings key/value store, without losing edits.
//
// Every dashboard feature store keeps a whole JSON document under one settings
// key. A plain read-then-write drops a concurrent edit wholesale — the later
// writer wins with a document that never saw the earlier one — and more than
// one dashboard replica against one backend is a supported deployment.
//
// `updateSetting` closes that: it writes conditionally on the value it read and
// re-reads on a lost race. Writes here are admin-frequency, so contention is
// rare and a retry is cheap.

/** The settings-KV surface a conditional update needs. */
export interface ConditionalSettings {
  getSetting(key: string): string | null;
  setSettingIf(key: string, expected: string | null, value: string): boolean;
}

/**
 * The whole settings surface a document store works through — narrower than the
 * native queue, so a store depends only on the KV it actually uses.
 */
export interface SettingsStore extends ConditionalSettings {
  setSetting(key: string, value: string): void;
  deleteSetting(key: string): boolean;
  listSettings(): Record<string, string>;
}

/**
 * How many times {@link updateSetting} re-reads and retries before giving up.
 *
 * A losing writer only ever loses to a writer that won, so the bound has to
 * clear the number of dashboards that could be editing one document at once.
 * Losing this many in a row is a fault, not contention worth waiting out.
 */
export const MAX_ATTEMPTS = 25;

/** Thrown when {@link updateSetting} lost {@link MAX_ATTEMPTS} races in a row. */
export class SettingConflictError extends Error {
  constructor(readonly key: string) {
    super(`setting '${key}' kept changing under a conditional write`);
    this.name = "SettingConflictError";
  }
}

/**
 * Load, mutate and store a JSON document, retrying if someone else wrote first.
 *
 * `load` turns the raw stored value (`null` when unset) into the document — the
 * same decoding each store already does, so a malformed row keeps reading as
 * empty. `mutate` must change the document **in place** and do nothing else: it
 * runs once per attempt. Its return value comes back from the winning attempt.
 */
export function updateSetting<Document, Outcome>(
  settings: ConditionalSettings,
  key: string,
  load: (raw: string | null) => Document,
  mutate: (document: Document) => Outcome,
): Outcome {
  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
    const stored = settings.getSetting(key);
    const document = load(stored);
    // Compared against the document as loaded, not against the raw stored
    // string: on a missing key the raw is null while the encoding is the empty
    // document, so comparing to the raw would read "changed nothing" as a
    // change and write a row for it.
    const before = JSON.stringify(document);
    const outcome = mutate(document);
    const after = JSON.stringify(document);
    if (after === before) {
      return outcome;
    }
    if (settings.setSettingIf(key, stored, after)) {
      return outcome;
    }
  }
  throw new SettingConflictError(key);
}
