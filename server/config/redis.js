import { createClient } from 'redis';
import { config } from './index.js';

export function createRedisClient(name, url = config.redisUrl) {
  const client = createClient({
    url,
    disableOfflineQueue: true,
    socket: {
      connectTimeout: 5000,
      reconnectStrategy: (attempt) => Math.min(100 * 2 ** Math.min(attempt, 5), 3000),
    },
  });
  client.on('error', (error) => console.error(`Redis ${name}: ${error.message}`));
  return client;
}

export async function closeRedis(client) {
  if (client.isReady) await client.quit();
  else if (client.isOpen) client.destroy();
}
