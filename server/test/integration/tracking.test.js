import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import { io } from 'socket.io-client';

const id = randomUUID().replaceAll('-', '');
process.env.KAFKA_LOCATION_TOPIC = `test.locations.${id}`;
process.env.KAFKA_LOCATION_GROUP = `test.worker.${id}`;
process.env.ACTIVITY_LOGS = 'false';
const { config } = await import('../../config/index.js');
const { kafka, connectKafkaProducer, disconnectKafkaProducer, stopKafkaConsumer } =
  await import('../../config/kafka.js');
const { connectDB, disconnectDB, getMongoClient } = await import('../../config/db.js');
const { createRedisClient, closeRedis } = await import('../../config/redis.js');
const { getLocationModel } = await import('../../location/model.js');
const { createLocationRepository } = await import('../../location/repository.js');
const { createLocationStore, locationKey } = await import('../../location/store.js');
const { createLocationLimiter } = await import('../../location/rate-limit.js');
const { createLocationService } = await import('../../location/service.js');
const { createLocationHandler } = await import('../../location/handler.js');
const { createWorkerStatus } = await import('../../location/worker-status.js');
const { publishEvent } = await import('../../producer/producer.js');
const { startConsumer } = await import('../../consumer/consumer.js');
const { createApp } = await import('../../app.js');
const { attachTrackingServer } = await import('../../realtime/tracking.js');
const { createAuthorizer } = await import('../../auth/tokens.js');
const { createApi } = await import('../../../client/src/lib/api.js');

async function eventually(predicate, message) {
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await delay(40);
  }
  assert.fail(message);
}

test(
  'two riders reach independent customers, Mongo and Redis; duplicates and worker outages are handled',
  { timeout: 90000 },
  async (t) => {
    const redis = createRedisClient('integration');
    const subscriber = createRedisClient('integration-sub');
    const admin = kafka.admin();
    const orderIds = [`integration-${id}-1`, `integration-${id}-2`];
    const status = createWorkerStatus(redis, config.kafka.locationGroup);
    let model;
    let consumer;
    let tracking;
    const sockets = [];
    t.after(async () => {
      sockets.forEach((socket) => socket.disconnect());
      await tracking?.close();
      await stopKafkaConsumer(consumer);
      await disconnectKafkaProducer();
      if (model) await model.collection.drop();
      await disconnectDB();
      await admin.deleteTopics({ topics: [config.kafka.locationTopic] }).catch(() => {});
      await admin.disconnect();
      if (redis.isReady) {
        await redis.del(
          orderIds.flatMap((orderId) => [locationKey(orderId), `tracking:{${orderId}}:rate`]),
        );
        await redis.del(`tracking:workers:${config.kafka.locationGroup}`);
      }
      await Promise.all([closeRedis(redis), closeRedis(subscriber)]);
    });
    await Promise.all([redis.connect(), subscriber.connect(), admin.connect(), connectDB()]);
    model = getLocationModel(getMongoClient().connection, `test_rider_locations_${id}`);
    await model.init();
    const repository = createLocationRepository(model);
    const store = createLocationStore(redis, config.location);
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic: config.kafka.locationTopic, numPartitions: 3, replicationFactor: 1 }],
    });
    await connectKafkaProducer();
    const processed = [];
    const handler = createLocationHandler({ store, repository, locationConfig: config.location });
    consumer = await startConsumer(
      async (event) => {
        processed.push(await handler(event));
      },
      {
        topic: config.kafka.locationTopic,
        groupId: config.kafka.locationGroup,
        fromBeginning: true,
      },
    );
    const authorize = createAuthorizer({ demo: true });
    const service = createLocationService({
      publish: publishEvent,
      store,
      limiter: createLocationLimiter(redis, 3000),
      locationConfig: config.location,
    });
    const httpServer = createServer(
      createApp({
        service,
        authorize,
        isReady: () => redis.isReady,
        isWorkerReady: () => status.isOnline(),
        minIntervalMs: 3000,
      }),
    );
    tracking = attachTrackingServer(httpServer, { subscriber, authorize });
    await new Promise((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
    const url = `http://127.0.0.1:${httpServer.address().port}`;
    const api = createApi({ baseUrl: url });
    const timestamp = Date.now();
    const points = orderIds.map((orderId, index) => ({
      orderId,
      riderId: `RIDER-0${index + 1}`,
      latitude: 28.63 + index,
      longitude: 77.21,
      timestamp,
    }));
    await assert.rejects(api.send(points[0]), { status: 503 });
    assert.equal((await fetch(`${url}/health/ready`)).status, 503);
    await status.announce('worker-a');
    await status.announce('worker-b');
    await status.remove('worker-a');
    assert.equal(await status.isOnline(), true, 'One worker stopping must not hide another');

    const events = [[], []];
    for (let i = 0; i < 2; i++) {
      const socket = io(url, {
        transports: ['websocket'],
        autoConnect: false,
        auth: { orderId: orderIds[i] },
      });
      socket.on('location:update', (point) => events[i].push(point));
      sockets.push(socket);
      const connected = new Promise((resolve, reject) => {
        socket.once('connect', resolve);
        socket.once('connect_error', reject);
      });
      socket.connect();
      await connected;
      assert.equal((await socket.timeout(5000).emitWithAck('tracking:subscribe')).ok, true);
    }
    await Promise.all(points.map((point) => api.send(point)));
    await eventually(
      () => events.every((list) => list.length === 1),
      'Both riders must reach their respective customers',
    );
    assert.equal(await model.countDocuments(), 2);
    for (let i = 0; i < 2; i++) {
      assert.equal(events[i][0].orderId, orderIds[i]);
      assert.equal((await repository.latest(orderIds[i])).riderId, points[i].riderId);
      assert.equal((await store.latest(orderIds[i])).timestamp, timestamp);
    }
    await publishEvent(config.kafka.locationTopic, points[1], { key: orderIds[1] });
    await publishEvent(
      config.kafka.locationTopic,
      { ...points[1], timestamp: timestamp - 1 },
      { key: orderIds[1] },
    );
    await eventually(() => processed.length === 4, 'Duplicate/old events should finish processing');
    assert.deepEqual(processed.slice(2), [false, false]);
    assert.equal((await repository.latest(orderIds[1])).timestamp, timestamp);
    assert.equal(events[1].length, 1);

    // A reconnect snapshot comes from Redis after the live subscription has been restored.
    sockets[1].disconnect();
    const newer = { ...points[1], timestamp: timestamp + 1 };
    await publishEvent(config.kafka.locationTopic, newer, { key: orderIds[1] });
    await eventually(
      async () => (await store.latest(orderIds[1])).timestamp === newer.timestamp,
      'Missed update must be cached',
    );
    const reconnected = new Promise((resolve) => sockets[1].once('connect', resolve));
    sockets[1].connect();
    await reconnected;
    await sockets[1].timeout(5000).emitWithAck('tracking:subscribe');
    assert.equal((await api.latest(orderIds[1])).location.timestamp, newer.timestamp);

    await Promise.all(
      [8, 4, 2, 7, 8].map((n) =>
        repository.saveIfNewer({ ...points[0], timestamp: timestamp + n }),
      ),
    );
    assert.equal((await repository.latest(orderIds[0])).timestamp, timestamp + 8);
    assert.equal(await model.countDocuments(), 2);
    await status.remove('worker-b');
    await assert.rejects(api.send({ ...points[0], timestamp: Date.now() }), { status: 503 });
  },
);
