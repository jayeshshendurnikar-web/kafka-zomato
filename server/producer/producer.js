import { getKafkaProducer } from '../config/kafka.js';
import { logActivity } from '../lib/activity.js';

export const publishEvent = async (topic, payload, { key = null, headers = {} } = {}) => {
  if (typeof topic !== 'string' || !topic.trim()) {
    throw new TypeError('Kafka topic must be a non-empty string');
  }

  if (payload === undefined || payload === null) {
    throw new TypeError('Kafka event payload is required');
  }

  const value = typeof payload === 'string' ? payload : JSON.stringify(payload);

  const message = {
    value,
    ...(key ? { key: String(key) } : {}),
    ...(Object.keys(headers).length > 0 ? { headers } : {}),
  };

  const producer = getKafkaProducer();
  logActivity('kafka.publish.started', { topic, orderId: key });
  const recordMetadata = await producer.send({
    topic,
    acks: -1,
    messages: [message],
  });
  logActivity('kafka.publish.accepted', { topic, orderId: key, metadata: recordMetadata });

  return {
    topic,
    key,
    metadata: recordMetadata,
  };
};

export const publishMessage = (payload, options = {}) =>
  publishEvent(options.topic, payload, options);
