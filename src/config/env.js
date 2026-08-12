// Central, environment-variable-driven configuration.
// Mirrors the Spring Boot application.yml -> @ConfigurationProperties pattern:
// no credential/connection-string/keystore value is ever hardcoded here.
require('dotenv').config();

function bool(value, fallback) {
  if (value === undefined || value === null || value === '') return fallback;
  return String(value).toLowerCase() === 'true';
}

function num(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

const config = {
  server: {
    port: num(process.env.PORT, 8082),
    env: process.env.NODE_ENV || 'development',
  },

  cors: {
    allowedOrigins: (process.env.ITEM_CORS_ALLOWED_ORIGINS || 'http://localhost:5173,http://localhost:3000')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
  },

  kafka: {
    bootstrapServers: (process.env.ITEM_KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092')
      .split(',')
      .map((s) => s.trim()),
    topic: process.env.ITEM_KAFKA_TOPIC || 'Item_Topic',
    clientId: process.env.ITEM_KAFKA_CLIENT_ID || 'item-kafka-producer-poc',
    consumerGroup: process.env.ITEM_KAFKA_CONSUMER_GROUP || 'item_group',
    manualConsumerGroup: process.env.ITEM_KAFKA_MANUAL_CONSUMER_GROUP || 'manual-item-group',
  },

  sourceDb: {
    type: (process.env.ITEM_SOURCE_DB_TYPE || 'mysql').toLowerCase(), // 'mysql' | 'mssql'
    mysql: {
      host: process.env.ITEM_MYSQL_SOURCE_HOST || 'localhost',
      port: num(process.env.ITEM_MYSQL_SOURCE_PORT, 3306),
      database: process.env.ITEM_MYSQL_SOURCE_DB || 'item_poc_source',
      user: process.env.ITEM_MYSQL_SOURCE_USERNAME || 'root',
      password: process.env.ITEM_MYSQL_SOURCE_PASSWORD || 'change-me',
      table: process.env.ITEM_MYSQL_SOURCE_TABLE || 'ITEM',
    },
    mssql: {
      host: process.env.ITEM_MSSQL_HOST || 'localhost',
      port: num(process.env.ITEM_MSSQL_PORT, 1433),
      database: process.env.ITEM_MSSQL_DB || 'item_poc_source',
      user: process.env.ITEM_MSSQL_USERNAME || 'sa',
      password: process.env.ITEM_MSSQL_PASSWORD || 'change-me',
      table: process.env.ITEM_MSSQL_SOURCE_TABLE || 'ITEM',
      encrypt: bool(process.env.ITEM_MSSQL_ENCRYPT, false),
      trustServerCertificate: bool(process.env.ITEM_MSSQL_TRUST_SERVER_CERT, true),
    },
  },

  sinkDb: {
    host: process.env.ITEM_MYSQL_HOST || 'localhost',
    port: num(process.env.ITEM_MYSQL_PORT, 3306),
    database: process.env.ITEM_MYSQL_DB || 'item_poc',
    user: process.env.ITEM_MYSQL_USERNAME || 'root',
    password: process.env.ITEM_MYSQL_PASSWORD || 'change-me',
    table: process.env.ITEM_MYSQL_TABLE || 'ITEM',
  },

  jwt: {
    privateKey: process.env.ITEM_JWT_PRIVATE_KEY || '',
    issuer: process.env.ITEM_JWT_ISSUER || 'item-kafka-producer',
    expiryMinutes: num(process.env.ITEM_JWT_EXPIRY_MINUTES, 30),
  },

  gateway: {
    url: process.env.ITEM_GATEWAY_URL || 'http://localhost:8081/item-kafka/app/send-items/v1',
  },

  job: {
    batchSize: num(process.env.ITEM_JOB_BATCH_SIZE, 1000),
    batchIntervalMs: num(process.env.ITEM_JOB_BATCH_INTERVAL_MS, 200),
    maxRetries: num(process.env.ITEM_JOB_MAX_RETRIES, 3),
  },

  logging: {
    level: process.env.LOG_LEVEL || 'info',
  },
};

module.exports = config;

