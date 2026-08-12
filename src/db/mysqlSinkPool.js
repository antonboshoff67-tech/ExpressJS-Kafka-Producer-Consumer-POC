const mysql = require('mysql2/promise');
const config = require('../config/env');

/**
 * Connection pool for the MySQL "sink" database (item_poc), written to by
 * the Kafka -> MySQL streaming job (kafkaItemToMysqlJob). Mirrors
 * spring.mysql.* in application.yml.
 */
const sinkPool = mysql.createPool({
  host: config.sinkDb.host,
  port: config.sinkDb.port,
  database: config.sinkDb.database,
  user: config.sinkDb.user,
  password: config.sinkDb.password,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  dateStrings: true,
});

module.exports = sinkPool;

