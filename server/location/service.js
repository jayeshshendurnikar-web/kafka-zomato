import { HttpError } from '../lib/errors.js';
import { validateLocation } from './validation.js';
import { TOPICS } from '../kafka/topics.js';
import { logActivity } from '../lib/activity.js';

export function createLocationService({ publish, store, limiter, locationConfig }) {
  return {
    latest: (orderId) => store.latest(orderId),
    async submit(input) {
      const location = validateLocation(input, locationConfig);
      logActivity('api.location.received', location);
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
