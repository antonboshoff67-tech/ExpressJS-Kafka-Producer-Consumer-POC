const app = require('./src/app');
const config = require('./src/config/env');
const logger = require('./src/utils/logger');

const server = app.listen(config.server.port, () => {
  logger.info(`item-kafka-producer-poc (Express.js) listening on port ${config.server.port}`);
  logger.info(`Swagger UI available at http://localhost:${config.server.port}/agent/swagger-ui.html`);
});

process.on('SIGINT', () => {
  logger.info('Shutting down (SIGINT)...');
  server.close(() => process.exit(0));
});

process.on('SIGTERM', () => {
  logger.info('Shutting down (SIGTERM)...');
  server.close(() => process.exit(0));
});

module.exports = server;

