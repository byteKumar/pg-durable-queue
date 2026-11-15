import { noop } from './noop.ts';
import { sleep } from './sleep.ts';
import type { Handler } from './types.ts';
import { writeEffect } from './write-effect.ts';

export type { Handler, HandlerContext } from './types.ts';

// Handler registry: job.type -> function that runs it.
export const defaultHandlers: Record<string, Handler> = {
  noop,
  sleep,
  'write-effect': writeEffect,
};
