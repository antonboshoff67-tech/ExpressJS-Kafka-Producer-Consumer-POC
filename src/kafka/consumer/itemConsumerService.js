const kafka = require('../kafkaClient');
const config = require('../../config/env');
const logger = require('../../utils/logger');

const MANUAL_POLL_TIMEOUT_MS = 30000;
const IDLE_STOP_MS = 5000; // stop early if nothing new arrives for this long

/**
 * On-demand, time-boxed Kafka consumption, mirroring
 * com.antontech.itemkafka_poc.kafka.consumer.ItemConsumerService. Unlike a
 * real long-lived consumer, this opens a consumer group, polls for up to
 * ~30 seconds (or stops early after a few seconds of inactivity) and closes.
 */
class ItemConsumerService {
  constructor() {
    this._running = false;
  }

  isRunning() {
    return this._running;
  }

  /**
   * @param {string} groupId - 'item_group' or 'manual-item-group'
   * @returns {Promise<string>} a summary message of how many items were consumed.
   */
  async manualConsume(groupId) {
    this._running = true;
    const consumer = kafka.consumer({ groupId: `${groupId}-manual-${Date.now()}` });
    let count = 0;
    let lastMessageAt = Date.now();

    try {
      await consumer.connect();
      await consumer.subscribe({ topic: config.kafka.topic, fromBeginning: true });

      await new Promise((resolve) => {
        let finished = false;
        const finish = async () => {
          if (finished) return;
          finished = true;
          try {
            await consumer.stop();
          } catch (e) {
            /* ignore */
          }
          resolve();
        };

        const hardTimeout = setTimeout(finish, MANUAL_POLL_TIMEOUT_MS);
        const idleInterval = setInterval(() => {
          if (Date.now() - lastMessageAt > IDLE_STOP_MS) {
            clearInterval(idleInterval);
            clearTimeout(hardTimeout);
            finish();
          }
        }, 500);

        consumer
          .run({
            eachMessage: async ({ message }) => {
              count++;
              lastMessageAt = Date.now();
              logger.debug(`Manually consumed item #${count}: ${message.value ? message.value.toString().slice(0, 200) : ''}`);
            },
          })
          .catch((err) => {
            logger.error(`Consumer run error: ${err.message}`);
            clearInterval(idleInterval);
            clearTimeout(hardTimeout);
            finish();
          });
      });
    } catch (err) {
      logger.error(`manualConsume failed: ${err.message}`, { stack: err.stack });
    } finally {
      try {
        await consumer.disconnect();
      } catch (e) {
        /* ignore */
      }
      this._running = false;
    }

    return `Manually consumed ${count} items.`;
  }
}

module.exports = new ItemConsumerService();

