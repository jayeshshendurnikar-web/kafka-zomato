import { createServer } from 'node:http';
import { config, validateRuntimeConfig } from './config/index.js';
import { connectKafkaProducer, disconnectKafkaProducer } from './config/kafka.js';
import { createRedisClient, closeRedis } from './config/redis.js';
import { publishEvent } from './producer/producer.js';
import { createLocationStore } from './location/store.js';
import { createLocationLimiter } from './location/rate-limit.js';
import { createLocationService } from './location/service.js';
import { createAuthorizer } from './auth/tokens.js';
import { attachTrackingServer } from './realtime/tracking.js';
import { createApp } from './app.js';
import { installShutdown } from './lib/lifecycle.js';
import { createWorkerStatus } from './location/worker-status.js';
import { GROUPS } from './kafka/groups.js';

validateRuntimeConfig();
const redis = createRedisClient('api');
const subscriber = createRedisClient('subscriber');
const authorize = createAuthorizer(config.auth);
const workerStatus = createWorkerStatus(redis, GROUPS.LOCATION_CACHE);
let producerReady = false;
const service = createLocationService({
  publish: publishEvent,
  store: createLocationStore(redis, config.location),
  limiter: createLocationLimiter(redis, config.location.minIntervalMs),
  locationConfig: config.location,
});
const app = createApp({
  service,
  authorize,
  isReady: () => producerReady && redis.isReady && subscriber.isReady,
  isWorkerReady: () => workerStatus.isOnline(),
  minIntervalMs: config.location.minIntervalMs,
  demo: config.auth.demo,
});
const httpServer = createServer(app);
const tracking = attachTrackingServer(httpServer, { subscriber, authorize });
const shutdown = installShutdown(async () => {
  producerReady = false;
  await tracking.close();
  await Promise.all([disconnectKafkaProducer(), closeRedis(redis), closeRedis(subscriber)]);
});

try {
  await redis.connect();
  await subscriber.connect();
  const producer = await connectKafkaProducer();
  producerReady = true;
  producer.on(producer.events.DISCONNECT, () => {
    producerReady = false;
  });
  producer.on(producer.events.CONNECT, () => {
    producerReady = true;
  });
  await new Promise((resolve, reject) => {
    httpServer.once('error', reject);
    httpServer.listen(config.port, config.host, resolve);
  });
  console.log(
    `Tracking app: http://${config.host}:${config.port}${config.auth.demo ? ' (local demo mode)' : ''}`,
  );
} catch (error) {
  console.error(`Startup failed: ${error.message}`);
  process.exitCode = 1;
  await shutdown('startup error');
}
