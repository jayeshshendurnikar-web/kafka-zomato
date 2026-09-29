import { validateLocation } from '../api/validation.js';
import { logActivity } from '../lib/activity.js';

// Core Consumer Message Handler:
// Consumes raw Kafka events, persists latest location to MongoDB, updates Redis cache & pub/sub
export function createLocationHandler({
  store,
  repository,
  locationConfig,
  activity = logActivity,
}) {
  return async ({ value, key, offset, partition }) => {
    let location;
    try {
      // Edge Case 1 & 2: Validate payload structure and ensure partition key strictly matches orderId.
      // maxAgeMs is set to Infinity here so delayed Kafka backlog events can still be safely archived into Mongo.
      location = validateLocation(value, { ...locationConfig, maxAgeMs: Infinity });
      if (key !== location.orderId) throw new Error('Kafka key must match orderId');
    } catch (error) {
      // Discard malformed/invalid messages immediately so they don't block the consumer loop
      activity(
        'worker.location.invalid',
        { orderId: key, offset, partition, reason: error.message },
        'warn',
      );
      return;
    }

    try {
      // Step 1: Atomic Redis Update & Pub/Sub (Real-time live map updates for customer)
      let saved = false;
      const isStale = location.timestamp < Date.now() - locationConfig.maxAgeMs;

      if (!isStale) {
        // Runs Lua script: compares existing cached timestamp -> sets latest key -> publishes to channel
        saved = await store.saveIfNewer(location);
      } else {
        // Stale Backlog Check: archived in Mongo later, but NOT republished as a live point
        activity('worker.location.expired_for_live', { ...location, offset, partition });
      }

      // Step 2: Persist to MongoDB (Durable storage & persistence)
      await repository.saveIfNewer(location);

      // Log successful consumption and processing with full coordinates
      activity('worker.location.processed', {
        orderId: location.orderId,
        riderId: location.riderId,
        latitude: location.latitude,
        longitude: location.longitude,
        timestamp: location.timestamp,
        offset,
        partition,
        saved,
      });

      return saved;
    } catch (error) {
      // Edge Case 4: Storage or Network Failure
      // Throwing error here prevents Kafka offset commit. Kafka will re-deliver the message.
      activity(
        'worker.location.retry',
        {
          orderId: location?.orderId,
          riderId: location?.riderId,
          latitude: location?.latitude,
          longitude: location?.longitude,
          offset,
          partition,
          reason: error.message,
        },
        'error',
      );
      throw error;
    }
  };
}
