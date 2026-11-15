import type { ClaimMode } from './queue.ts';

// Reads a positive integer from the environment, or returns the default.
export function envInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer, got "${raw}"`);
  }
  return value;
}

export function claimModeFromEnv(): ClaimMode {
  const mode = process.env.CLAIM_MODE ?? 'skip_locked';
  if (mode !== 'skip_locked' && mode !== 'naive') {
    throw new Error(`CLAIM_MODE must be skip_locked or naive, got "${mode}"`);
  }
  return mode;
}
