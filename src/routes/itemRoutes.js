const express = require('express');
const router = express.Router();
const itemRepository = require('../repositories/itemRepository');
const logger = require('../utils/logger');

/**
 * @openapi
 * /item-kafka/app/items/v1:
 *   get:
 *     summary: List items (paginated)
 *     tags: [Items]
 *     parameters:
 *       - in: query
 *         name: page
 *         schema: { type: integer, default: 0 }
 *       - in: query
 *         name: size
 *         schema: { type: integer, default: 15 }
 *     responses:
 *       200: { description: A page of Item rows }
 */
router.get('/items/v1', async (req, res) => {
  const page = parseInt(req.query.page, 10) || 0;
  const size = parseInt(req.query.size, 10) || 15;
  try {
    const { content, totalElements } = await itemRepository.findAllPaged(page, size);
    const totalPages = Math.ceil(totalElements / size);
    logger.debug(`Returning items page ${page} of size ${size} (totalElements=${totalElements})`);
    res.json({
      content,
      totalElements,
      totalPages,
      number: page,
      size,
      first: page === 0,
      last: page >= totalPages - 1,
    });
  } catch (err) {
    logger.error(`listItems failed: ${err.message}`, { stack: err.stack });
    res.status(500).json({ error: err.message });
  }
});

/**
 * @openapi
 * /item-kafka/app/items/count/v1:
 *   get:
 *     summary: Count items
 *     tags: [Items]
 *     responses:
 *       200: { description: Total number of Item rows in the source table }
 */
router.get('/items/count/v1', async (req, res) => {
  try {
    const total = await itemRepository.count();
    res.json(total);
  } catch (err) {
    logger.error(`countItems failed: ${err.message}`, { stack: err.stack });
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;

