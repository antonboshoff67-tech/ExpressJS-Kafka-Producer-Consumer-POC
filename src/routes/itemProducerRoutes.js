const express = require('express');
const router = express.Router();
const itemRepository = require('../repositories/itemRepository');
const itemProducerService = require('../kafka/producer/itemProducerService');
const logger = require('../utils/logger');

/**
 * @openapi
 * /item-kafka/app/publish-items/v1:
 *   post:
 *     summary: Read items and publish them to Kafka
 *     tags: [Items]
 *     responses:
 *       200: { description: Items sent to Kafka topic successfully }
 */
router.post('/publish-items/v1', async (req, res) => {
  try {
    const items = await itemRepository.findFirst100ByItemIdIsNotNull();
    if (!items || items.length === 0) {
      logger.warn('No items found to publish');
    } else {
      logger.debug(`Items size = ${items.length}`);
    }
    await itemProducerService.sendItems(items || []);
    res.type('text').send('Items sent to Kafka topic successfully!');
  } catch (err) {
    logger.error(`createItemKafkaTopic failed: ${err.message}`, { stack: err.stack });
    res.status(500).type('text').send('Error occurred in createItemKafkaTopic!');
  }
});

module.exports = router;

