import { useState } from "react";

/** Copies a snippet to the clipboard, confirming in the label for a beat. Used
 *  by every code pane on the landing page. */
export function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);

  // Confirm only what actually happened. `navigator.clipboard` is absent in any
  // non-secure context and `writeText` rejects when the permission is denied —
  // in both cases the snippet is still on screen to select by hand, but a label
  // reading "Copied" tells the reader it is on their clipboard when it is not.
  async function copy() {
    const clipboard = navigator.clipboard;
    if (!clipboard) {
      return;
    }
    try {
      await clipboard.writeText(text);
    } catch {
      return;
    }
    setCopied(true);
    setTimeout(() => setCopied(false), 1300);
  }

  return (
    <button
      type="button"
      className="hcopy"
      onClick={() => {
        void copy();
      }}
    >
      <span className="lbl">{copied ? "Copied" : "Copy"}</span>
    </button>
  );
}
