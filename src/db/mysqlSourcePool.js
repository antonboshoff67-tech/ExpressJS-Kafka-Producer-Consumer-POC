const mysql = require('mysql2/promise');
const config = require('../config/env');

/**
 * Connection pool for the MySQL "source" database (item_poc_source),
 * used when ITEM_SOURCE_DB_TYPE=mysql (the default so this POC works
 * end-to-end without SQL Server). Mirrors spring.datasource.* in application.yml.
 */
const sourcePool = mysql.createPool({
  host: config.sourceDb.mysql.host,
  port: config.sourceDb.mysql.port,
  database: config.sourceDb.mysql.database,
  user: config.sourceDb.mysql.user,
  password: config.sourceDb.mysql.password,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  dateStrings: true,
});

module.exports = sourcePool;

