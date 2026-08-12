const axios = require('axios');
const config = require('../config/env');
const jwtTokenUtil = require('../utils/jwtTokenUtil');
const logger = require('../utils/logger');

/**
 * Demonstrates JWT-signed calls to an external gateway, mirrors
 * com.antontech.itemkafka_poc.service.impl.MsgRoutingServiceImpl.
 */
class MsgRoutingService {
  /**
   * Builds a signed JWT and forwards the message to the configured gateway endpoint.
   * @param {object} serviceRequest
   */
  async processSentMsgRequest(serviceRequest) {
    const token = jwtTokenUtil.generateToken({ sub: 'item-kafka-producer-poc' });
    logger.debug(`Prepared request for gateway ${config.gateway.url} with signed JWT.`);
    try {
      await axios.post(config.gateway.url, serviceRequest, {
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        timeout: 5000,
      });
    } catch (err) {
      // In a POC/demo environment the downstream gateway is frequently not
      // running - log and swallow so the caller still gets a clean 200.
      logger.warn(`Gateway call failed (this is expected if no gateway is running): ${err.message}`);
    }
  }

  /**
   * Simulates handling of a message "received" from the Kafka consumer side of the test flow.
   * @param {object} serviceRequest
   */
  async processReceivedMsgRequest(serviceRequest) {
    logger.debug(`Received message request: ${JSON.stringify(serviceRequest)}`);
  }
}

module.exports = new MsgRoutingService();

