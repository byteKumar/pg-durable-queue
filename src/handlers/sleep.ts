import { payloadMs, sleep as wait, type Handler } from './types.ts';

// Waits payload.ms milliseconds: stands in for a job that calls a slow external service.
export const sleep: Handler = async (job) => {
  await wait(payloadMs(job));
};
