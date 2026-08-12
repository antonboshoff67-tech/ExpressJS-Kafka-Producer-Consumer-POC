const express = require('express');
const router = express.Router();
const msgRoutingService = require('../services/msgRoutingService');
const logger = require('../utils/logger');

/**
 * @openapi
 * /item-kafka/app/send-items/v1:
 *   post:
 *     summary: Send a message to the Kafka producer gateway flow (JWT-signed)
 *     tags: [Gateway]
 *     responses:
 *       200: { description: Message prepared for publishing }
 *       400: { description: There was a problem }
 */
router.post('/send-items/v1', async (req, res) => {
  try {
    await msgRoutingService.processSentMsgRequest(req.body);
    res.status(200).send('The items were prepared for publishing to the Kafka topic.');
  } catch (err) {
    logger.error(`sendItemsToKafka failed: ${err.message}`, { stack: err.stack });
    res.status(400).send(`There was a problem: ${err.message}`);
  }
});

/**
 * @openapi
 * /item-kafka/app/consume-items/v1:
 *   get:
 *     summary: Receive messages from the Kafka consumer test flow
 *     tags: [Gateway]
 *     responses:
 *       200: { description: Message processed successfully }
 *       400: { description: There was a problem }
 */
router.get('/consume-items/v1', async (req, res) => {
  const authToken = req.headers.authorization;
  if (authToken) {
    logger.debug('Authorization header received for consume request');
  }
  try {
    await msgRoutingService.processReceivedMsgRequest(req.body);
    res.status(200).send('Message items were processed successfully from the Kafka consumer.');
  } catch (err) {
    logger.error(`consumeItemsFromKafka failed: ${err.message}`, { stack: err.stack });
    res.status(400).send(`There was a problem: ${err.message}`);
  }
});

module.exports = router;

