import { logActivity } from './activity.js';
export function createLocationState(orderId, onChange) {
  let current = null;
  return {
    get current() {
      return current;
    },
    accept(location) {
      if (
        !location ||
        location.orderId !== orderId ||
        !Number.isSafeInteger(location.timestamp) ||
        location.timestamp <= 0 ||
        !Number.isFinite(location.latitude) ||
        Math.abs(location.latitude) > 90 ||
        !Number.isFinite(location.longitude) ||
        Math.abs(location.longitude) > 180 ||
        (current && location.timestamp <= current.timestamp)
      ) {
        if (location)
          logActivity('customer.location.ignored', {
            orderId,
            timestamp: location.timestamp,
            currentTimestamp: current?.timestamp,
          });
        return false;
      }
      current = location;
      logActivity('customer.location.rendered', location);
      onChange(location);
      return true;
    },
  };
}
