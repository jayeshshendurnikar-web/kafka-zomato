import { Kafka, logLevel, Partitioners } from 'kafkajs';
import { config } from './index.js';

export const kafka = new Kafka({
  clientId: config.kafka.clientId,
  brokers: config.kafka.brokers,
  logLevel: logLevel.WARN,
  connectionTimeout: 5000,
  requestTimeout: 10000,
  retry: { retries: 8 },
});

let producer;
let connecting;
const consumers = new Set();

export async function connectKafkaProducer() {
  if (producer) return producer;
  if (!connecting) {
    connecting = (async () => {
      const client = kafka.producer({
        createPartitioner: Partitioners.DefaultPartitioner,
        idempotent: true,
        maxInFlightRequests: 1,
        allowAutoTopicCreation: false,
      });
      try {
        await client.connect();
        producer = client;
        return client;
      } catch (error) {
        await client.disconnect().catch(() => {});
        throw error;
      }
    })().finally(() => {
      connecting = null;
    });
  }
  return connecting;
}

export function getKafkaProducer() {
  if (!producer) throw new Error('Kafka producer is not connected');
  return producer;
}

export async function disconnectKafkaProducer() {
  await producer?.disconnect();
  producer = null;
}

export async function createKafkaConsumer(groupId) {
  if (typeof groupId !== 'string' || !groupId.trim())
    throw new TypeError('A non-empty groupId is required');
  const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false });
  try {
    await consumer.connect();
    consumers.add(consumer);
    return consumer;
  } catch (error) {
    await consumer.disconnect().catch(() => {});
    throw error;
  }
}

export async function stopKafkaConsumer(consumer) {
  if (!consumer) return;
  try {
    await consumer.stop();
    await consumer.disconnect();
  } finally {
    consumers.delete(consumer);
  }
}

export const ensureKafkaTopics = async (definitions) => {
  if (!Array.isArray(definitions)) {
    throw new TypeError('Topic definitions must be an array');
  }
  if (definitions.length === 0) return;

  const admin = kafka.admin();
  await admin.connect();
  try {
    const existing = new Set(await admin.listTopics());
    const missing = definitions
      .filter(({ name }) => !existing.has(name))
      .map(({ name, numPartitions, replicationFactor, configEntries }) => ({
        topic: name,
        numPartitions,
        replicationFactor,
        ...(configEntries ? { configEntries } : {}),
      }));

    if (missing.length > 0) {
      await admin.createTopics({
        topics: missing,
        waitForLeaders: true,
      });
    }
  } finally {
    await admin.disconnect();
  }
};
