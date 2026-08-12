const { Kafka, logLevel } = require('kafkajs');
const config = require('../config/env');

/**
 * Shared KafkaJS client, equivalent in spirit to Spring Kafka's
 * auto-configured ProducerFactory/ConsumerFactory beans (ReactiveKafkaConfig
 * in the Spring Boot POC). Bootstrap servers come from ITEM_KAFKA_BOOTSTRAP_SERVERS.
 */
const kafka = new Kafka({
  clientId: config.kafka.clientId,
  brokers: config.kafka.bootstrapServers,
  logLevel: logLevel.NOTHING,
  retry: {
    initialRetryTime: 300,
    retries: 5,
  },
});

module.exports = kafka;

