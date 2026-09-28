import { logActivity } from '../lib/activity.js';

export const logKafkaEvent = ({ groupId, topic, partition, offset, key, value }) =>
  logActivity('kafka.consume.received', {
    groupId,
    topic,
    partition,
    offset,
    orderId: key,
    riderId: value?.riderId,
    timestamp: value?.timestamp,
  });

export const logMessage = logKafkaEvent;
