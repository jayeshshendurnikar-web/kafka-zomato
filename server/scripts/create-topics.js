import { ensureKafkaTopics } from '../config/kafka.js';
import { config } from '../config/index.js';
import { TOPICS } from '../kafka/topics.js';

await ensureKafkaTopics([
  {
    name: TOPICS.RIDER_LOCATION,
    numPartitions: config.kafka.partitions,
    replicationFactor: config.kafka.replicationFactor,
    configEntries: [{ name: 'retention.ms', value: '86400000' }],
  },
]);
console.log(`Topic ready: ${TOPICS.RIDER_LOCATION}`);
