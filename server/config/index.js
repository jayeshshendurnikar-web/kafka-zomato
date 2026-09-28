import dotenv from 'dotenv';
import { fileURLToPath } from 'node:url';

dotenv.config({ path: fileURLToPath(new URL('../.env', import.meta.url)), quiet: true });

const positiveInteger = (name, fallback) => {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isSafeInteger(value) || value < 1)
    throw new Error(`${name} must be a positive integer`);
  return value;
};

export const config = {
  port: positiveInteger('PORT', 5000),
  host: process.env.HOST || '127.0.0.1',
  mongoUri: process.env.MONGODB_URI,
  kafka: {
    clientId: process.env.KAFKA_CLIENT_ID || 'kafka-practice-server-zomato',
    brokers: (process.env.KAFKA_BROKERS || `localhost:${process.env.KAFKA_PORT || '9092'}`)
      .split(',')
      .map((broker) => broker.trim())
      .filter(Boolean),
    locationTopic: process.env.KAFKA_LOCATION_TOPIC || 'rider.locations.v1',
    locationGroup: process.env.KAFKA_LOCATION_GROUP || 'location-cache-v1',
    partitions: positiveInteger('KAFKA_PARTITIONS', 6),
    replicationFactor: positiveInteger('KAFKA_REPLICATION_FACTOR', 1),
  },
  redisUrl: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
  location: {
    ttlSeconds: positiveInteger('LOCATION_TTL_SECONDS', 86400),
    maxAgeMs: positiveInteger('LOCATION_MAX_AGE_MS', 300000),
    maxFutureMs: positiveInteger('LOCATION_MAX_FUTURE_MS', 30000),
    minIntervalMs: positiveInteger('LOCATION_MIN_INTERVAL_MS', 3000),
  },
  auth: {
    demo: process.env.DEMO_MODE === 'true',
    secret: process.env.TRACKING_TOKEN_SECRET || '',
  },
};

export function validateRuntimeConfig() {
  if (config.auth.demo && process.env.NODE_ENV === 'production') {
    throw new Error('DEMO_MODE is disabled in production');
  }
  if (!config.auth.demo && config.auth.secret.length < 32) {
    throw new Error(
      'Set TRACKING_TOKEN_SECRET (at least 32 characters), or DEMO_MODE=true locally',
    );
  }
  if (config.location.ttlSeconds * 1000 <= config.location.maxAgeMs + config.location.maxFutureMs) {
    throw new Error('LOCATION_TTL_SECONDS must exceed the accepted timestamp window');
  }
}
