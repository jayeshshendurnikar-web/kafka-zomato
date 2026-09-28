import { logActivity } from '../lib/activity.js';

// Kafka Consumer Logger: Logs events when consumed by consumer worker
export const logKafkaEvent = ({ groupId, topic, partition, offset, key, value }) =>
  logActivity('kafka.consume.received', {
    groupId,
    topic,
    partition,
    offset,
    orderId: key,
    riderId: value?.riderId,
    latitude: value?.latitude,
    longitude: value?.longitude,
    timestamp: value?.timestamp,
  });
