import { describe } from "@effect/vitest";

import { DrahtOrchestratorReplayHarness } from "../Adapters/DrahtAdapterV2.testkit.ts";
import { itRunsReplayFixtures, runReplayFixture } from "./ProviderReplayFixtures.ts";

describe("Draht replay fixtures", () => {
  itRunsReplayFixtures("draht", (input) =>
    runReplayFixture({ ...input, harness: DrahtOrchestratorReplayHarness }),
  );
});
