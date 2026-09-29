import { randomUUID } from 'node:crypto';
import { config, validateRuntimeConfig } from '../config/index.js';
import { createRedisClient, closeRedis } from '../config/redis.js';
import { stopKafkaConsumer } from '../config/kafka.js';
import { startConsumer } from './consumer.js';
import { createLocationStore } from '../storage/redis/store.js';
import { createLocationHandler } from './handler.js';
import { TOPICS } from '../kafka/topics.js';
import { GROUPS } from '../kafka/groups.js';
import { installShutdown } from '../lib/lifecycle.js';
import { connectDB, disconnectDB, getMongoClient } from '../config/db.js';
import { getLocationModel } from '../storage/mongo/model.js';
import { createLocationRepository } from '../storage/mongo/repository.js';
import { createLocationBatcher } from './batcher.js';
import { createWorkerStatus } from './worker-status.js';
import { logActivity } from '../lib/activity.js';

validateRuntimeConfig();

const redis = createRedisClient('worker');
let consumer;
let heartbeat;
let batcher;
let consuming = false;
const workerId = randomUUID();
const status = createWorkerStatus(redis, GROUPS.LOCATION_CACHE);

const shutdown = installShutdown(async () => {
  consuming = false;
  clearInterval(heartbeat);
  await status.remove(workerId).catch(() => {});
  await stopKafkaConsumer(consumer);
  if (batcher) await batcher.stop().catch(() => {});
  await Promise.all([closeRedis(redis), disconnectDB()]);
});

try {
  await redis.connect();
  await connectDB();

  const model = getLocationModel(getMongoClient().connection);
  await model.init();

  logActivity('mongo.ready', {
    database: model.db.name,
    collection: model.collection.collectionName,
  });

  const repository = createLocationRepository(model);
  batcher = createLocationBatcher({
    repository,
    batchSize: config.location.batchSize,
    flushIntervalMs: config.location.batchIntervalMs,
  });

  // Start Kafka consumer group runner with our message handler
  consumer = await startConsumer(
    createLocationHandler({
      store: createLocationStore(redis, config.location),
      repository,
      batcher,
      locationConfig: config.location,
    }),
    { topic: TOPICS.RIDER_LOCATION, groupId: GROUPS.LOCATION_CACHE, fromBeginning: true },
  );

  // Heartbeat lease management: signals to API instances that a worker is actively consuming
  const updateStatus = async () => {
    try {
      if (consuming && getMongoClient().connection.readyState === 1) {
        await status.announce(workerId);
      } else {
        await status.remove(workerId);
      }
    } catch (error) {
      logActivity('worker.heartbeat.failed', { reason: error.message }, 'warn');
    }
  };

  consumer.on(consumer.events.GROUP_JOIN, () => {
    consuming = true;
    void updateStatus();
  });

  consumer.on(consumer.events.STOP, () => {
    consuming = false;
    void updateStatus();
  });

  consumer.on(consumer.events.DISCONNECT, () => {
    consuming = false;
    void updateStatus();
  });

  consumer.on(consumer.events.CRASH, ({ payload }) => {
    consuming = false;
    void updateStatus();
    logActivity(
      'worker.consumer.crashed',
      { restarting: payload.restart, reason: payload.error?.message },
      'error',
    );
    if (!payload.restart) {
      console.error('Consumer stopped; exiting for supervisor restart');
      process.exitCode = 1;
      void shutdown('consumer failure');
    }
  });

  consuming = true;
  await updateStatus();
  heartbeat = setInterval(updateStatus, 5000);

  logActivity('worker.ready', {
    workerId,
    topic: TOPICS.RIDER_LOCATION,
    groupId: GROUPS.LOCATION_CACHE,
  });
} catch (error) {
  console.error(`Worker startup failed: ${error.message}`);
  process.exitCode = 1;
  await shutdown('startup error');
}
