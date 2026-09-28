import { validateLocation } from './validation.js';
import { logActivity } from '../lib/activity.js';

export function createLocationHandler({
  store,
  repository,
  locationConfig,
  activity = logActivity,
}) {
  return async ({ value, key, offset, partition }) => {
    let location;
    try {
      // Delayed Kafka deliveries are still durable in Mongo, but must not revive a stale map point.
      location = validateLocation(value, { ...locationConfig, maxAgeMs: Infinity });
      if (key !== location.orderId) throw new Error('Kafka key must match orderId');
    } catch (error) {
      activity(
        'worker.location.invalid',
        { orderId: key, offset, partition, reason: error.message },
        'warn',
      );
      return;
    }
    try {
      // Always retry Mongo writes, even if a prior attempt already updated Redis.
      await repository.saveIfNewer(location);
      if (location.timestamp < Date.now() - locationConfig.maxAgeMs) {
        activity('worker.location.expired_for_live', { ...location, offset });
        return false;
      }
      const saved = await store.saveIfNewer(location);
      activity('worker.location.processed', {
        orderId: location.orderId,
        riderId: location.riderId,
        timestamp: location.timestamp,
        offset,
        partition,
        saved,
      });
      return saved;
    } catch (error) {
      activity(
        'worker.location.retry',
        { orderId: location.orderId, offset, partition, reason: error.message },
        'error',
      );
      // No Kafka acknowledgment until BOTH persistence and cache processing finish.
      throw error;
    }
  };
}
