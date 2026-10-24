import { ProviderDriverKind } from "@t3tools/contracts";
import { defineProviderClient } from "@t3tools/provider-core/client";

import { DrahtSettings } from "./settings.ts";

export const drahtClient = defineProviderClient({
  driverKind: ProviderDriverKind.make("draht"),
  label: "Draht",
  settingsSchema: DrahtSettings,
  icon: {
    viewBox: "0 0 24 24",
    fill: { light: "#0F0F0F", dark: "#F5F5F5" },
    paths: [
      {
        d: "M3 2H12A10 10 0 0 1 12 22H3ZM7.5 6.5V17.5H12A5.5 5.5 0 0 0 12 6.5Z",
        fillRule: "evenodd",
      },
    ],
  },
});
