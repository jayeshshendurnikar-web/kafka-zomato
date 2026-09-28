import { getKafkaProducer } from '../config/kafka.js';
import { logActivity } from '../lib/activity.js';

// Kafka Event Producer: Publishes rider location updates to Kafka topics
export const publishEvent = async (topic, payload, { key = null, headers = {} } = {}) => {
  // Edge Case 1: Validate topic string
  if (typeof topic !== 'string' || !topic.trim()) {
    throw new TypeError('Kafka topic must be a non-empty string');
  }

  // Edge Case 2: Validate payload presence
  if (payload === undefined || payload === null) {
    throw new TypeError('Kafka event payload is required');
  }

  // Kafka messages require string or buffer values
  const value = typeof payload === 'string' ? payload : JSON.stringify(payload);
  const payloadObj = typeof payload === 'object' && payload !== null ? payload : null;

  // Edge Case 3: Partition key is crucial (orderId) to ensure in-order delivery per order
  const message = {
    value,
    ...(key ? { key: String(key) } : {}),
    ...(Object.keys(headers).length > 0 ? { headers } : {}),
  };

  const producer = getKafkaProducer();

  // Log before sending to Kafka including exact GPS coordinates
  logActivity('kafka.publish.started', {
    topic,
    orderId: key,
    riderId: payloadObj?.riderId,
    latitude: payloadObj?.latitude,
    longitude: payloadObj?.longitude,
    timestamp: payloadObj?.timestamp,
  });

  // Produce event with acks: -1 (all ISR replicas must acknowledge before success)
  const recordMetadata = await producer.send({
    topic,
    acks: -1,
    messages: [message],
  });

  // Log confirmation once Kafka broker accepts the event
  logActivity('kafka.publish.accepted', {
    topic,
    orderId: key,
    riderId: payloadObj?.riderId,
    latitude: payloadObj?.latitude,
    longitude: payloadObj?.longitude,
    partition: recordMetadata[0]?.partition,
    offset: recordMetadata[0]?.baseOffset,
  });

  return {
    topic,
    key,
    metadata: recordMetadata,
  };
};
