import { useState } from "react";

/** Copies a snippet to the clipboard, confirming in the label for a beat. Used
 *  by every code pane on the landing page. */
export function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      className="hcopy"
      onClick={() => {
        navigator.clipboard?.writeText(text);
        setCopied(true);
        setTimeout(() => setCopied(false), 1300);
      }}
    >
      <span className="lbl">{copied ? "Copied" : "Copy"}</span>
    </button>
  );
}
