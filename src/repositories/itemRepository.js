const config = require('../config/env');
const Item = require('../models/Item');
const logger = require('../utils/logger');
const mysqlSourcePool = require('../db/mysqlSourcePool');
const { getMssqlPool, sql } = require('../db/mssqlSourcePool');

/**
 * Reactive-free repository for the Item source table, abstracting over
 * MySQL (default) or real MS SQL Server (ITEM_SOURCE_DB_TYPE=mssql).
 * Mirrors com.antontech.itemkafka_poc.repos.ItemRepository.
 */
class ItemRepository {
  /** @returns {Promise<Item[]>} the first 100 items with a non-null itemId, ordered by itemId. */
  async findFirst100ByItemIdIsNotNull() {
    const table = this._table();
    if (config.sourceDb.type === 'mssql') {
      const pool = await getMssqlPool();
      const result = await pool.request().query(`SELECT TOP 100 * FROM ${table} WHERE item_id IS NOT NULL ORDER BY item_id ASC`);
      return result.recordset.map(Item.fromDbRow);
    }
    const [rows] = await mysqlSourcePool.query(
      `SELECT * FROM ${table} WHERE item_id IS NOT NULL ORDER BY item_id ASC LIMIT 100`
    );
    return rows.map(Item.fromDbRow);
  }

  /** @returns {Promise<{content: Item[], totalElements: number}>} a single page of items, sorted by itemId. */
  async findAllPaged(page = 0, size = 15) {
    const table = this._table();
    const offset = page * size;
    if (config.sourceDb.type === 'mssql') {
      const pool = await getMssqlPool();
      const countResult = await pool.request().query(`SELECT COUNT(*) AS total FROM ${table}`);
      const dataResult = await pool
        .request()
        .input('offset', sql.Int, offset)
        .input('size', sql.Int, size)
        .query(`SELECT * FROM ${table} ORDER BY item_id ASC OFFSET @offset ROWS FETCH NEXT @size ROWS ONLY`);
      return {
        content: dataResult.recordset.map(Item.fromDbRow),
        totalElements: countResult.recordset[0].total,
      };
    }
    const [countRows] = await mysqlSourcePool.query(`SELECT COUNT(*) AS total FROM ${table}`);
    const [rows] = await mysqlSourcePool.query(
      `SELECT * FROM ${table} ORDER BY item_id ASC LIMIT ? OFFSET ?`,
      [size, offset]
    );
    return { content: rows.map(Item.fromDbRow), totalElements: countRows[0].total };
  }

  /** @returns {Promise<number>} the total number of Item rows in the source table. */
  async count() {
    const table = this._table();
    if (config.sourceDb.type === 'mssql') {
      const pool = await getMssqlPool();
      const result = await pool.request().query(`SELECT COUNT(*) AS total FROM ${table}`);
      return result.recordset[0].total;
    }
    const [rows] = await mysqlSourcePool.query(`SELECT COUNT(*) AS total FROM ${table}`);
    return rows[0].total;
  }

  _table() {
    return config.sourceDb.type === 'mssql' ? config.sourceDb.mssql.table : config.sourceDb.mysql.table;
  }
}

logger.debug(`ItemRepository initialised with source type: ${config.sourceDb.type}`);

module.exports = new ItemRepository();

