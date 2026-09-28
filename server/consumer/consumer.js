import { createKafkaConsumer, stopKafkaConsumer } from '../config/kafka.js';
import { logKafkaEvent } from '../kafka/logger.js';

export const startConsumer = async (
  handler,
  { topic, topics: subscribedTopics, groupId, fromBeginning = true } = {},
) => {
  if (typeof handler !== 'function') {
    throw new TypeError('Consumer message handler must be a function');
  }

  const topicNames = subscribedTopics ?? [topic];
  if (
    !Array.isArray(topicNames) ||
    topicNames.length === 0 ||
    topicNames.some((name) => typeof name !== 'string' || !name.trim())
  ) {
    throw new TypeError('Provide a non-empty topic or topics array');
  }

  if (typeof groupId !== 'string' || !groupId.trim()) {
    throw new TypeError('Consumer groupId must be a non-empty string');
  }

  const consumer = await createKafkaConsumer(groupId);
  let timeoutId;
  let removeGroupListener;

  try {
    await consumer.subscribe({ topics: topicNames, fromBeginning });

    const groupJoined = new Promise((resolve, reject) => {
      timeoutId = setTimeout(() => {
        reject(new Error(`Kafka consumer group join timed out for group '${groupId}'`));
      }, 30000);
      removeGroupListener = consumer.on(consumer.events.GROUP_JOIN, resolve);
    });

    const running = consumer.run({
      eachMessage: async ({ topic: messageTopic, partition, message }) => {
        const rawValue = message.value?.toString() ?? '';
        let parsedValue = rawValue;

        try {
          parsedValue = JSON.parse(rawValue);
        } catch {
          // Keep string if not valid JSON
        }

        const eventData = {
          groupId,
          topic: messageTopic,
          partition,
          offset: message.offset,
          key: message.key?.toString() ?? null,
          timestamp: message.timestamp,
          value: parsedValue,
        };

        try {
          logKafkaEvent(eventData);
          await handler(eventData);
        } catch (handlerError) {
          console.error(
            `❌ Error in consumer handler for group '${groupId}' on topic '${messageTopic}' (offset: ${message.offset}):`,
            handlerError,
          );
          // Let Kafka retry; resolving here would commit a location lost during a Redis outage.
          // Domain handlers explicitly discard malformed events before doing I/O.
          throw handlerError;
        }
      },
    });

    await Promise.all([running, groupJoined]);
    return consumer;
  } catch (error) {
    await stopKafkaConsumer(consumer).catch(() => {});
    throw error;
  } finally {
    clearTimeout(timeoutId);
    removeGroupListener?.();
  }
};
