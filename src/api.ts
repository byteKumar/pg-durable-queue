import { fileURLToPath } from 'node:url';
import Fastify, { type FastifyInstance, type FastifyServerOptions } from 'fastify';
import { envInt } from './config.ts';
import { createPool, type Pool } from './db.ts';
import { Metrics } from './metrics.ts';
import { enqueue, type Job } from './queue.ts';

interface EnqueueBody {
  type: string;
  payload?: unknown;
  queue?: string;
  priority?: number;
  runAt?: string;
  idempotencyKey?: string;
  maxAttempts?: number;
}

const enqueueSchema = {
  body: {
    type: 'object',
    required: ['type'],
    additionalProperties: false,
    properties: {
      type: { type: 'string', minLength: 1, maxLength: 200 },
      payload: {}, // any JSON value
      // The queue name becomes the NOTIFY channel "jobs_<queue>". Channel names are limited
      // to 63 bytes, so keep it short and ASCII.
      queue: { type: 'string', pattern: '^[A-Za-z0-9_-]{1,50}$' },
      priority: { type: 'integer', minimum: -2_147_483_648, maximum: 2_147_483_647 }, // int4
      runAt: { type: 'string', format: 'date-time' },
      idempotencyKey: { type: 'string', minLength: 1, maxLength: 200 },
      maxAttempts: { type: 'integer', minimum: 1, maximum: 100 },
    },
  },
} as const;

const getJobSchema = {
  params: {
    type: 'object',
    required: ['id'],
    properties: { id: { type: 'integer', minimum: 1, maximum: Number.MAX_SAFE_INTEGER } },
  },
} as const;

export interface ApiOptions {
  pool: Pool;
  logger?: FastifyServerOptions['logger'];
}

export function buildApi({ pool, logger = false }: ApiOptions): FastifyInstance {
  const app = Fastify({
    logger,
    ajv: {
      // Fastify's default silently drops unknown fields. Reject them instead: a client that
      // sends "idempotency_key" by mistake must get a 400, not a job with no dedupe.
      customOptions: { removeAdditional: false },
    },
  });
  const metrics = new Metrics();
  metrics.enableQueueDepth(pool);

  app.post<{ Body: EnqueueBody }>('/jobs', { schema: enqueueSchema }, async (req, reply) => {
    const body = req.body;
    const { id, created } = await enqueue(pool, {
      type: body.type,
      payload: body.payload,
      queue: body.queue,
      priority: body.priority,
      runAt: body.runAt === undefined ? undefined : new Date(body.runAt),
      idempotencyKey: body.idempotencyKey,
      maxAttempts: body.maxAttempts,
    });
    // 201 for a new job; 200 when the idempotency key matched an existing one.
    return reply.code(created ? 201 : 200).send({ id });
  });

  app.get<{ Params: { id: number } }>('/jobs/:id', { schema: getJobSchema }, async (req, reply) => {
    const { rows } = await pool.query<Job>('SELECT * FROM jobs WHERE id = $1', [req.params.id]);
    if (!rows[0]) return reply.code(404).send({ error: 'job not found' });
    return rows[0];
  });

  app.get('/health', async (_req, reply) => {
    try {
      await pool.query('SELECT 1');
      return { status: 'ok' };
    } catch {
      return reply.code(503).send({ status: 'database unreachable' });
    }
  });

  app.get('/metrics', async (_req, reply) => {
    return reply.type(metrics.registry.contentType).send(await metrics.registry.metrics());
  });

  return app;
}

// CLI: `node src/api.ts`
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const pool = createPool(undefined, envInt('PG_POOL_MAX', 10));
  pool.on('error', (err) => console.error(`idle connection error: ${err.message}`));
  const app = buildApi({ pool, logger: { level: process.env.LOG_LEVEL ?? 'info' } });

  const shutdown = async () => {
    await app.close(); // stops accepting connections, finishes in-flight requests
    await pool.end();
    process.exit(0);
  };
  process.once('SIGTERM', () => void shutdown());
  process.once('SIGINT', () => void shutdown());

  await app.listen({ port: envInt('PORT', 3000), host: process.env.HOST ?? '0.0.0.0' });
}
