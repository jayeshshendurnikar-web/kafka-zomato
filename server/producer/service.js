import { HttpError } from '../lib/errors.js';
import { validateLocation } from '../api/validation.js';
import { TOPICS } from '../kafka/topics.js';
import { logActivity } from '../lib/activity.js';

// Producer Service: Validates incoming location, applies rate limiting, and publishes to Kafka
export function createLocationService({ publish, store, limiter, locationConfig }) {
  return {
    latest: (orderId) => store.latest(orderId),
    async submit(input) {
      // Step 1: Validate payload and coordinates
      const location = validateLocation(input, locationConfig);
      logActivity('api.location.received', location);

      // Step 2: Rate limit per orderId (Redis-backed token acquire)
      const release = await limiter.acquire(location.orderId);
      if (!release) {
        logActivity(
          'api.location.rate_limited',
          { orderId: location.orderId, riderId: location.riderId },
          'warn',
        );
        throw new HttpError(
          429,
          `Send location updates at most once every ${locationConfig.minIntervalMs / 1000} seconds`,
        );
      }

      // Step 3: Publish to Kafka with orderId as partition key
      try {
        await publish(TOPICS.RIDER_LOCATION, location, { key: location.orderId });
      } catch (error) {
        await release().catch(() => {});
        throw error;
      }
      return location;
    },
  };
}
