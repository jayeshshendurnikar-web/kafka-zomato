import { logActivity } from '../lib/activity.js';
export const locationKey = (orderId) => `tracking:{${orderId}}:latest`;
export const locationChannel = (orderId) => `tracking:{${orderId}}:updates`;

// Compare + cache + publish in ONE Redis operation. Duplicate Kafka deliveries are harmless.
export const SAVE_LOCATION_SCRIPT = `
local previous = redis.call('GET', KEYS[1])
if previous and tonumber(cjson.decode(previous).timestamp) >= tonumber(ARGV[1]) then
  return 0
end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
redis.call('PUBLISH', ARGV[4], ARGV[2])
return 1
`;

export function createLocationStore(redis, { ttlSeconds }, activity = logActivity) {
  return {
    async latest(orderId) {
      const value = await redis.get(locationKey(orderId));
      activity('redis.location.read', { orderId, found: Boolean(value) });
      return value ? JSON.parse(value) : null;
    },
    async saveIfNewer(location) {
      const saved = await redis.eval(SAVE_LOCATION_SCRIPT, {
        keys: [locationKey(location.orderId)],
        arguments: [
          String(location.timestamp),
          JSON.stringify(location),
          String(ttlSeconds),
          locationChannel(location.orderId),
        ],
      });
      activity(saved === 1 ? 'redis.location.saved_and_published' : 'redis.location.ignored', {
        ...location,
        key: locationKey(location.orderId),
        channel: locationChannel(location.orderId),
      });
      return saved === 1;
    },
  };
}
