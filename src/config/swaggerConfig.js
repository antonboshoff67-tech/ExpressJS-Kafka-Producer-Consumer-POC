const swaggerJsdoc = require('swagger-jsdoc');

const options = {
  definition: {
    openapi: '3.0.0',
    info: {
      title: 'Item Kafka Producer/Consumer POC (Express.js)',
      version: '1.0.0',
      description:
        'Express.js/Node.js replica of the Spring Boot Item Kafka Producer/Consumer + Apache Flink POC. ' +
        'REST API + KafkaJS producer/consumer + Node-based streaming pipeline jobs + MySQL/SQL Server integration.',
      contact: { name: 'antonboshoff67-tech' },
      license: { name: 'MIT' },
    },
    servers: [{ url: '/' }],
  },
  apis: ['./src/routes/*.js'],
};

module.exports = swaggerJsdoc(options);

