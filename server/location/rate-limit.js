import { randomUUID } from 'node:crypto';

const RELEASE_SCRIPT = `
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`;

export function createLocationLimiter(redis, intervalMs) {
  return {
    async acquire(orderId) {
      const key = `tracking:{${orderId}}:rate`;
      const token = randomUUID();
      const acquired = await redis.set(key, token, { NX: true, PX: intervalMs });
      return acquired
        ? async () => redis.eval(RELEASE_SCRIPT, { keys: [key], arguments: [token] })
        : null;
    },
  };
}
