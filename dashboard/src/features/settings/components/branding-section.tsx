import { useEffect, useState } from "react";
import {
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Input,
} from "@/components/ui";
import { useDeleteSetting, useUpdateSetting } from "../hooks";
import { SETTING_KEYS, type SettingsSnapshot } from "../types";
import { SettingRow } from "./setting-row";

/**
 * Branding overrides — the dashboard title shown in the sidebar and the
 * accent color CSS variable. Empty inputs revert to the bundled defaults
 * by deleting the underlying setting key.
 */
export function BrandingSection({ settings }: { settings: SettingsSnapshot }) {
  const update = useUpdateSetting();
  const remove = useDeleteSetting();

  const [title, setTitle] = useState("");
  const [accent, setAccent] = useState("");

  useEffect(() => {
    setTitle(settings[SETTING_KEYS.brandTitle] ?? "");
    setAccent(settings[SETTING_KEYS.brandAccent] ?? "");
  }, [settings]);

  const onSave = () => {
    if (title) update.mutate({ key: SETTING_KEYS.brandTitle, value: title });
    else remove.mutate(SETTING_KEYS.brandTitle);
    if (accent) update.mutate({ key: SETTING_KEYS.brandAccent, value: accent });
    else remove.mutate(SETTING_KEYS.brandAccent);
  };

  const dirty =
    title !== (settings[SETTING_KEYS.brandTitle] ?? "") ||
    accent !== (settings[SETTING_KEYS.brandAccent] ?? "");

  return (
    <Card>
      <CardHeader>
        <CardTitle>Branding</CardTitle>
        <CardDescription>
          Override the dashboard name and accent color for this deployment.
        </CardDescription>
      </CardHeader>
      <CardContent className="divide-y divide-[var(--border)]">
        <SettingRow
          label="Instance name"
          description="Shown in the sidebar header and browser tab. Leave blank for the default."
          htmlFor="brand-title"
        >
          <Input
            id="brand-title"
            placeholder="FlexiQ"
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            maxLength={64}
          />
        </SettingRow>
        <SettingRow
          label="Accent color"
          description="Hex color for buttons, active links, and highlights (e.g. #6366f1)."
          htmlFor="brand-accent"
        >
          <div className="flex items-center gap-2">
            <input
              type="color"
              aria-label="Accent color swatch"
              value={/^#[0-9a-fA-F]{6}$/.test(accent) ? accent : "#6366f1"}
              onChange={(e) => setAccent(e.target.value)}
              className="size-9 shrink-0 cursor-pointer rounded-[var(--btn-radius)] border border-[var(--border-strong)] bg-transparent p-0.5"
            />
            <Input
              id="brand-accent"
              placeholder="#6366f1"
              value={accent}
              onChange={(e) => setAccent(e.target.value)}
              maxLength={9}
              className="font-mono"
            />
          </div>
        </SettingRow>
        <div className="flex justify-end pt-4">
          <Button onClick={onSave} disabled={!dirty || update.isPending}>
            Save branding
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
