import { type ModelCapabilities, type ProviderOptionChoice } from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as Predicate from "effect/Predicate";

/**
 * Draht's full thinking ladder. Extra High (`xhigh`) and Max are opt-in per
 * model via `thinkingLevelMap`; advertising them globally makes
 * `set_thinking_level` fail on models that lack them.
 */
const DRAHT_THINKING_LEVELS = ["off", "minimal", "low", "medium", "high", "xhigh", "max"] as const;

export type DrahtThinkingLevel = (typeof DRAHT_THINKING_LEVELS)[number];

const DRAHT_THINKING_LEVEL_LABELS: Record<DrahtThinkingLevel, string> = {
  off: "Off",
  minimal: "Minimal",
  low: "Low",
  medium: "Medium",
  high: "High",
  xhigh: "Extra High",
  max: "Max",
};

export const EMPTY_DRAHT_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

export function thinkingCapabilitiesForDrahtModel(
  model: unknown,
  defaultThinkingLevel: unknown,
): ModelCapabilities {
  const levels = supportedDrahtThinkingLevelsFromModel(model);
  if (levels.length === 0) return EMPTY_DRAHT_MODEL_CAPABILITIES;
  const defaultLevel = clampDrahtThinkingLevel(defaultThinkingLevel, levels);
  return createModelCapabilities({
    optionDescriptors: [
      {
        id: "thinking",
        label: "Thinking",
        type: "select",
        options: levels.map((level): ProviderOptionChoice => ({
          id: level,
          label: DRAHT_THINKING_LEVEL_LABELS[level],
          ...(level === defaultLevel ? { isDefault: true } : {}),
        })),
      },
    ],
  });
}

function clampDrahtThinkingLevel(
  input: unknown,
  availableLevels: ReadonlyArray<DrahtThinkingLevel>,
): DrahtThinkingLevel | undefined {
  if (typeof input !== "string") return undefined;
  const requestedIndex = DRAHT_THINKING_LEVELS.findIndex((level) => level === input);
  if (requestedIndex === -1) return undefined;
  const exact = availableLevels.find((level) => level === input);
  if (exact !== undefined) return exact;
  for (let index = requestedIndex + 1; index < DRAHT_THINKING_LEVELS.length; index += 1) {
    const higher = availableLevels.find((level) => level === DRAHT_THINKING_LEVELS[index]);
    if (higher !== undefined) return higher;
  }
  for (let index = requestedIndex - 1; index >= 0; index -= 1) {
    const lower = availableLevels.find((level) => level === DRAHT_THINKING_LEVELS[index]);
    if (lower !== undefined) return lower;
  }
  return availableLevels[0];
}

function supportedDrahtThinkingLevelsFromModel(model: unknown): ReadonlyArray<DrahtThinkingLevel> {
  if (recordField(model, "reasoning") !== true) return [];
  const thinkingLevelMap = thinkingLevelMapFromModel(model);
  return DRAHT_THINKING_LEVELS.filter((level) => {
    const mapped = thinkingLevelMap?.[level];
    if (mapped === null) return false;
    if (level === "xhigh" || level === "max") return mapped !== undefined;
    return true;
  });
}

function thinkingLevelMapFromModel(model: unknown): Record<string, unknown> | undefined {
  const value = recordField(model, "thinkingLevelMap");
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  return value as Record<string, unknown>;
}

function recordField(input: unknown, key: string): unknown {
  return Predicate.isObject(input) ? input[key] : undefined;
}
