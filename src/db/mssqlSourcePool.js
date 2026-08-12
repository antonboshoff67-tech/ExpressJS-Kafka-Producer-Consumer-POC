const sql = require('mssql');
const config = require('../config/env');

let poolPromise = null;

/**
 * Lazily-created connection pool for the optional real MS SQL Server source
 * database, used when ITEM_SOURCE_DB_TYPE=mssql. Mirrors
 * MSSQLDataSourceProperties in the Spring Boot POC.
 */
function getMssqlPool() {
  if (!poolPromise) {
    poolPromise = sql.connect({
      server: config.sourceDb.mssql.host,
      port: config.sourceDb.mssql.port,
      database: config.sourceDb.mssql.database,
      user: config.sourceDb.mssql.user,
      password: config.sourceDb.mssql.password,
      options: {
        encrypt: config.sourceDb.mssql.encrypt,
        trustServerCertificate: config.sourceDb.mssql.trustServerCertificate,
      },
      pool: { max: 10, min: 0, idleTimeoutMillis: 30000 },
    });
  }
  return poolPromise;
}

module.exports = { getMssqlPool, sql };

