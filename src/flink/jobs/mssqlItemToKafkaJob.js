const config = require('../../config/env');
const logger = require('../../utils/logger');
const Item = require('../../models/Item');
const kafka = require('../../kafka/kafkaClient');
const mysqlSourcePool = require('../../db/mysqlSourcePool');
const { getMssqlPool } = require('../../db/mssqlSourcePool');

const MAX_ROWS_PER_RUN = 100;

/**
 * Node.js equivalent of the Flink batch job MssqlItemToKafkaJob:
 * Source = MS SQL Server / MySQL "ITEM" source table, Sink = Kafka topic.
 * <p>
 * Note on parity with the Java POC: Apache Flink has no official Node.js
 * client, so this job re-implements the same *semantics* (bounded read of up
 * to 100 rows -> publish each as JSON to Kafka) directly in Node using the
 * kafkajs producer, rather than embedding a JVM-based Flink
 * StreamExecutionEnvironment. See ARCHITECTURE.md section "Why not a real
 * Flink cluster from Node.js?" for the full rationale and how to swap in a
 * real Flink job jar if you need one.
 * <p>
 * Kick off via POST /flink/start-job1.
 */
class MssqlItemToKafkaJob {
  async run() {
    logger.debug('Starting MssqlItemToKafkaJob to read from the source DB and publish to Kafka.');

    let items = [];
    try {
      items = await this._fetchItems();
      logger.debug(`Fetched ${items.length} items from source table.`);
    } catch (err) {
      logger.error(`Error fetching data from source DB: ${err.message}`, { stack: err.stack });
      throw err;
    }

    const producer = kafka.producer();
    await producer.connect();
    try {
      for (const item of items) {
        const json = JSON.stringify(item);
        logger.debug(`Publishing Item JSON to Kafka: ${json}`);
        await producer.send({
          topic: config.kafka.topic,
          messages: [{ key: item.itemId, value: json }],
        });
      }
      logger.debug('MssqlItemToKafkaJob executed successfully.');
    } finally {
      await producer.disconnect();
    }
  }

  async _fetchItems() {
    if (config.sourceDb.type === 'mssql') {
      const pool = await getMssqlPool();
      const table = config.sourceDb.mssql.table;
      const result = await pool.request().query(`SELECT TOP ${MAX_ROWS_PER_RUN} * FROM ${table}`);
      return result.recordset.map(Item.fromDbRow);
    }
    const table = config.sourceDb.mysql.table;
    const [rows] = await mysqlSourcePool.query(`SELECT * FROM ${table} LIMIT ?`, [MAX_ROWS_PER_RUN]);
    return rows.map(Item.fromDbRow);
  }
}

module.exports = MssqlItemToKafkaJob;

