const LEASE_MS = 15000;

// Worker Lease Status: Each worker has its own lease in Redis, so shutting down one instance cannot hide another.
export function createWorkerStatus(redis, groupId) {
  const key = `tracking:workers:${groupId}`;
  return {
    async announce(workerId) {
      const now = Date.now();
      await redis
        .multi()
        .zRemRangeByScore(key, '-inf', now)
        .zAdd(key, [{ score: now + LEASE_MS, value: workerId }])
        .pExpire(key, LEASE_MS * 2)
        .exec();
    },
    remove: (workerId) => redis.zRem(key, workerId),
    async isOnline() {
      return (await redis.zCount(key, Date.now() + 1, '+inf')) > 0;
    },
  };
}
