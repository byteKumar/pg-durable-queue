import type { Handler } from './types.ts';

// Does nothing. Used to measure the queue's own overhead.
export const noop: Handler = async () => {};
