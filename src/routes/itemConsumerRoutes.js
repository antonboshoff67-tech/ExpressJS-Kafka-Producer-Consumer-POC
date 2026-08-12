const express = require('express');
const router = express.Router();
const itemConsumerService = require('../kafka/consumer/itemConsumerService');

const ITEM_AUTO_GROUP = 'item_group';
const ITEM_MANUAL_GROUP = 'manual-item-group';
const INCORRECT_CONSUMER_GROUP_PASSED = 'Incorrect consumer group. Use item_group or manual-item-group.';

/**
 * @openapi
 * /item-kafka/consumer/consume-status/v1:
 *   get:
 *     summary: Check the current status of item consumption
 *     tags: [Items]
 *     responses:
 *       200: { description: Whether a continuous consumer is currently running }
 */
router.get('/consume-status/v1', (req, res) => {
  res.json(itemConsumerService.isRunning() ? 'Consumer is actively listening.' : 'Consumer is not running.');
});

/**
 * @openapi
 * /item-kafka/consumer/manual-consume/v1:
 *   post:
 *     summary: Manually consume items from the Kafka topic
 *     tags: [Items]
 *     requestBody:
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               groupId: { type: string, example: item_group }
 *     responses:
 *       200: { description: Summary of how many items were consumed }
 */
router.post('/manual-consume/v1', async (req, res) => {
  const groupId = (req.body && req.body.groupId) || '';
  if (groupId.toLowerCase() === ITEM_AUTO_GROUP || groupId.toLowerCase() === ITEM_MANUAL_GROUP) {
    const result = await itemConsumerService.manualConsume(groupId);
    return res.type('text').send(result);
  }
  res.type('text').send(INCORRECT_CONSUMER_GROUP_PASSED);
});

module.exports = router;

