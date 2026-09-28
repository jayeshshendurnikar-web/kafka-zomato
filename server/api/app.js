import express from 'express';
import { fileURLToPath } from 'node:url';
import { bearerToken } from '../auth/tokens.js';
import { validateId } from './validation.js';
import { logActivity } from '../lib/activity.js';

export function createApp({
  service,
  authorize,
  isReady,
  isWorkerReady = async () => true,
  minIntervalMs,
  demo = false,
}) {
  const app = express();
  app.disable('x-powered-by');

  // Request latency and completion logging
  app.use((request, response, next) => {
    const started = Date.now();
    response.on('finish', () => {
      if (request.path.startsWith('/api/') || response.statusCode >= 400) {
        logActivity('http.completed', {
          method: request.method,
          path: request.path,
          status: response.statusCode,
          durationMs: Date.now() - started,
        });
      }
    });
    next();
  });

  app.use(express.json({ limit: '4kb' }));

  // Liveness check (process alive)
  app.get('/health/live', (_request, response) => response.json({ status: 'ok' }));

  // Readiness check (Kafka producer + Redis + Worker online)
  app.get('/health/ready', async (_request, response) => {
    const worker = isReady() && (await isWorkerReady());
    response
      .status(worker ? 200 : 503)
      .json({ status: worker ? 'ready' : 'unavailable', worker: worker ? 'online' : 'offline' });
  });

  // Client configuration
  app.get('/api/config', (_request, response) => response.json({ demo, minIntervalMs }));

  // Middleware guarding all order tracking endpoints against infrastructure downtime
  app.use('/api/orders', (_request, response, next) => {
    response.set('Cache-Control', 'no-store');
    if (!isReady()) return response.status(503).json({ error: 'Tracking temporarily unavailable' });
    next();
  });

  // Rider submits updated coordinates
  app.post('/api/orders/:orderId/location', async (request, response) => {
    const orderId = validateId(request.params.orderId);
    authorize(bearerToken(request), { role: 'rider', orderId, riderId: request.body?.riderId });
    if (!(await isWorkerReady())) {
      logActivity(
        'api.worker.unavailable',
        { orderId, action: 'Start npm run dev (API + worker), or npm run worker separately' },
        'warn',
      );
      return response
        .status(503)
        .json({ error: 'Location processing is unavailable. Please try again shortly.' });
    }
    const location = await service.submit({ ...request.body, orderId });
    response.status(202).json({ accepted: true, timestamp: location.timestamp });
  });

  // Customer fetches latest known location snapshot
  app.get('/api/orders/:orderId/location', async (request, response) => {
    const orderId = validateId(request.params.orderId);
    authorize(bearerToken(request), { role: 'customer', orderId });
    const location = await service.latest(orderId);
    response.json({ location });
  });

  // Serve compiled frontend assets
  app.use(express.static(fileURLToPath(new URL('../../client/dist', import.meta.url))));

  app.use((_request, response) => response.status(404).json({ error: 'Not found' }));

  // Global HTTP error handler
  app.use((error, request, response, _next) => {
    const status = error.status || 503;
    logActivity(
      'http.failed',
      { method: request.method, path: request.path, status, reason: error.message },
      'error',
    );
    if (status === 429) response.set('Retry-After', String(Math.ceil(minIntervalMs / 1000)));
    response
      .status(status)
      .json({ error: status >= 500 ? 'Tracking temporarily unavailable' : error.message });
  });

  return app;
}
