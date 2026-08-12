const express = require('express');
const swaggerUi = require('swagger-ui-express');
const corsMiddleware = require('./config/corsConfig');
const swaggerSpec = require('./config/swaggerConfig');
const logger = require('./utils/logger');

const itemRoutes = require('./routes/itemRoutes');
const itemProducerRoutes = require('./routes/itemProducerRoutes');
const itemConsumerRoutes = require('./routes/itemConsumerRoutes');
const msgConsumerRoutes = require('./routes/msgConsumerRoutes');
const flinkJobRoutes = require('./routes/flinkJobRoutes');

const app = express();

app.use(corsMiddleware);
app.use(express.json());

app.use((req, res, next) => {
  logger.debug(`${req.method} ${req.originalUrl}`);
  next();
});

// Swagger UI, equivalent to springdoc.swagger-ui.path=/agent/swagger-ui.html
app.use('/agent/swagger-ui.html', swaggerUi.serve, swaggerUi.setup(swaggerSpec));

// Simple health check (equivalent to Spring Boot Actuator's /actuator/health)
app.get('/actuator/health', (req, res) => res.json({ status: 'UP' }));

// Routes - path prefixes mirror the Spring Boot @RequestMapping values exactly
app.use('/item-kafka/app', itemRoutes);
app.use('/item-kafka/app', itemProducerRoutes);
app.use('/item-kafka/app', msgConsumerRoutes);
app.use('/item-kafka/consumer', itemConsumerRoutes);
app.use('/flink', flinkJobRoutes);

// 404 fallback
app.use((req, res) => {
  res.status(404).json({ error: 'Not Found', path: req.originalUrl });
});

// Central error handler
app.use((err, req, res, next) => {
  logger.error(`Unhandled error: ${err.message}`, { stack: err.stack });
  res.status(500).json({ error: 'Internal Server Error' });
});

module.exports = app;

