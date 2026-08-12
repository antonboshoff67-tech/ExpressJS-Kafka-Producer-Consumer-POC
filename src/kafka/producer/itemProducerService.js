const kafka = require('../kafkaClient');
const config = require('../../config/env');
const logger = require('../../utils/logger');

const ITEM_AUTO_GROUP = 'item_group';
const ITEM_MANUAL_GROUP = 'manual-item-group';

let producer = null;
async function getProducer() {
  if (!producer) {
    producer = kafka.producer();
    await producer.connect();
  }
  return producer;
}

/**
 * Publishes Item records to the shared Kafka topic.
 * Mirrors com.antontech.itemkafka_poc.kafka.producer.ItemProducerService.
 */
class ItemProducerService {
  /**
   * Publishes the given items to Kafka, splitting them roughly in half so
   * the first half is keyed for the "auto" consumer group and the second
   * half for the "manual" consumer group.
   * @param {import('../../models/Item')[]} items
   */
  async sendItems(items) {
    const midpoint = Math.floor(items.length / 2);
    for (let i = 0; i < items.length; i++) {
      const groupId = i < midpoint ? ITEM_AUTO_GROUP : ITEM_MANUAL_GROUP;
      await this._sendItemWithGroupId(items[i], groupId);
    }
  }

  async _sendItemWithGroupId(item, groupId) {
    try {
      const p = await getProducer();
      const jsonItem = JSON.stringify(item);
      await p.send({
        topic: config.kafka.topic,
        messages: [{ key: `${groupId}_${item.itemId}`, value: jsonItem }],
      });
      logger.info(`Sent item ${item.itemId} to Kafka group ${groupId}`);
    } catch (err) {
      logger.error(`Failed to send item ${item.itemId}: ${err.message}`, { stack: err.stack });
    }
  }

  async disconnect() {
    if (producer) {
      await producer.disconnect();
      producer = null;
    }
  }
}

module.exports = new ItemProducerService();

