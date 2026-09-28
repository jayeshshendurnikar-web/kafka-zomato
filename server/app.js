import express from 'express';
import { fileURLToPath } from 'node:url';
import { bearerToken } from './auth/tokens.js';
import { validateId } from './location/validation.js';
import { logActivity } from './lib/activity.js';

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
  app.get('/health/live', (_request, response) => response.json({ status: 'ok' }));
  app.get('/health/ready', async (_request, response) => {
    const worker = isReady() && (await isWorkerReady());
    response
      .status(worker ? 200 : 503)
      .json({ status: worker ? 'ready' : 'unavailable', worker: worker ? 'online' : 'offline' });
  });
  app.get('/api/config', (_request, response) => response.json({ demo, minIntervalMs }));
  app.use('/api/orders', (_request, response, next) => {
    response.set('Cache-Control', 'no-store');
    if (!isReady()) return response.status(503).json({ error: 'Tracking temporarily unavailable' });
    next();
  });
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
  app.get('/api/orders/:orderId/location', async (request, response) => {
    const orderId = validateId(request.params.orderId);
    authorize(bearerToken(request), { role: 'customer', orderId });
    const location = await service.latest(orderId);
    response.json({ location });
  });
  app.use(express.static(fileURLToPath(new URL('../client/dist', import.meta.url))));
  app.use((_request, response) => response.status(404).json({ error: 'Not found' }));
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
