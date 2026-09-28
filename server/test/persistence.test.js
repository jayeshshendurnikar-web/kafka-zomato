import test from 'node:test';
import assert from 'node:assert/strict';
import { createLocationHandler } from '../location/handler.js';
import { createLocationRepository } from '../location/repository.js';
import { createLocationState } from '../../client/src/lib/location-state.js';

const point = {
  orderId: 'ORDER-2',
  riderId: 'RIDER-02',
  latitude: 28.63,
  longitude: 77.21,
  timestamp: Date.now(),
};
const locationConfig = { maxAgeMs: 300000, maxFutureMs: 30000 };
const activity = () => {};

test('worker persists Mongo before publishing a live update', async () => {
  const calls = [];
  const handler = createLocationHandler({
    locationConfig,
    activity,
    repository: {
      saveIfNewer: async () => {
        calls.push('mongo');
      },
    },
    store: {
      saveIfNewer: async () => {
        calls.push('redis');
        return true;
      },
    },
  });
  assert.equal(await handler({ value: point, key: point.orderId }), true);
  assert.deepEqual(calls, ['mongo', 'redis']);
});

test('Mongo failure rejects the handler so Kafka can retry without publishing prematurely', async () => {
  let cached = false;
  const handler = createLocationHandler({
    locationConfig,
    activity,
    repository: {
      saveIfNewer: async () => {
        throw new Error('Mongo offline');
      },
    },
    store: {
      saveIfNewer: async () => {
        cached = true;
      },
    },
  });
  await assert.rejects(handler({ value: point, key: point.orderId }), /Mongo offline/);
  assert.equal(cached, false);
});

test('retry still writes Redis when Mongo already persisted the same timestamp', async () => {
  let writes = 0;
  const handler = createLocationHandler({
    locationConfig,
    activity,
    repository: { saveIfNewer: async () => false },
    store: {
      saveIfNewer: async () => {
        if (++writes === 1) throw new Error('Redis offline');
        return true;
      },
    },
  });
  await assert.rejects(handler({ value: point, key: point.orderId }), /Redis offline/);
  assert.equal(await handler({ value: point, key: point.orderId }), true);
  assert.equal(writes, 2);
});

test('expired backlog is persisted in Mongo but does not reappear on the live map', async () => {
  let archived = false;
  let cached = false;
  const handler = createLocationHandler({
    locationConfig,
    activity,
    repository: {
      saveIfNewer: async () => {
        archived = true;
      },
    },
    store: {
      saveIfNewer: async () => {
        cached = true;
      },
    },
  });
  await handler({ value: { ...point, timestamp: Date.now() - 600000 }, key: point.orderId });
  assert.equal(archived, true);
  assert.equal(cached, false);
});

test('invalid messages never write Mongo or Redis', async () => {
  let writes = 0;
  const handler = createLocationHandler({
    locationConfig,
    activity,
    repository: {
      saveIfNewer: async () => {
        writes++;
      },
    },
    store: {
      saveIfNewer: async () => {
        writes++;
      },
    },
  });
  await handler({ value: { ...point, latitude: 100 }, key: point.orderId });
  await handler({ value: point, key: 'OTHER' });
  assert.equal(writes, 0);
});

test('concurrent Mongo insert retries the timestamp condition without another upsert', async () => {
  const calls = [];
  const repository = createLocationRepository(
    {
      collection: { collectionName: 'test' },
      async updateOne(filter, update, options) {
        calls.push({ filter, update, options });
        if (calls.length === 1) throw Object.assign(new Error('race'), { code: 11000 });
        return { modifiedCount: 1, upsertedCount: 0 };
      },
    },
    activity,
  );
  assert.equal(await repository.saveIfNewer(point), true);
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[0].filter, calls[1].filter);
  assert.equal(calls[0].options.upsert, true);
  assert.equal(calls[1].options.upsert, undefined);
});

test('customer ignores another order, an old point and a duplicate', () => {
  const rendered = [];
  const state = createLocationState(point.orderId, (location) => rendered.push(location));
  state.accept(point);
  state.accept({ ...point, orderId: 'ORDER-1', timestamp: point.timestamp + 1 });
  state.accept({ ...point, timestamp: point.timestamp - 1 });
  state.accept(point);
  assert.equal(rendered.length, 1);
});
