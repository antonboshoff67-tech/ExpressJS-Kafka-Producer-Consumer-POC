const config = require('../../config/env');
const logger = require('../../utils/logger');
const Item = require('../../models/Item');
const { ITEM_COLUMNS } = require('../../models/itemColumns');
const kafka = require('../../kafka/kafkaClient');
const sinkPool = require('../../db/mysqlSinkPool');

/**
 * Node.js equivalent of the Flink streaming job KafkaItemToMysqlJob:
 * Source = Kafka topic (Item JSON messages), Sink = MySQL ITEM table
 * (upsert via INSERT ... ON DUPLICATE KEY UPDATE).
 * <p>
 * This is a genuine unbounded stream, just like the Flink version: it keeps
 * consuming from Kafka until the process stops or is explicitly cancelled.
 * Records are buffered and flushed as a batch either when the buffer reaches
 * ITEM_JOB_BATCH_SIZE or every ITEM_JOB_BATCH_INTERVAL_MS - the direct
 * equivalent of Flink's JdbcExecutionOptions.withBatchSize/.withBatchIntervalMs
 * used by the Java job, giving the same "batch many small upserts into fewer,
 * larger round trips" behaviour without needing a JVM/Flink runtime.
 * <p>
 * Kick off via POST /flink/start-job2. Stop via POST /flink/stop-job2.
 */
class KafkaItemToMysqlJob {
  constructor() {
    this._consumer = null;
    this._buffer = [];
    this._flushTimer = null;
    this._stopped = true;
  }

  /** @returns {Promise<void>} resolves once the Kafka subscription is established and the stream has started running. */
  async run() {
    logger.debug(`KafkaItemToMysqlJob: starting to consume from Kafka topic ${config.kafka.topic}.`);
    this._stopped = false;
    this._consumer = kafka.consumer({ groupId: config.kafka.consumerGroup });
    await this._consumer.connect();
    await this._consumer.subscribe({ topic: config.kafka.topic, fromBeginning: true });

    this._flushTimer = setInterval(() => {
      this._flush().catch((err) => logger.error(`Scheduled flush failed: ${err.message}`));
    }, config.job.batchIntervalMs);

    // Run in the background - do not await consumer.run() (it never resolves
    // while messages keep flowing), mirroring the unbounded nature of the
    // Flink KafkaSource in the original job. The caller only awaits until
    // the subscription + run loop has been kicked off.
    this._consumer
      .run({
        eachMessage: async ({ message }) => {
          try {
            const json = message.value ? message.value.toString() : null;
            if (!json) return;
            const item = Item.fromJson(JSON.parse(json)).applyDefaults();
            this._buffer.push(item);
            if (this._buffer.length >= config.job.batchSize) {
              await this._flush();
            }
          } catch (err) {
            logger.error(`Failed to process Kafka message: ${err.message}`, { stack: err.stack });
          }
        },
      })
      .catch((err) => {
        logger.error(`KafkaItemToMysqlJob run loop failed: ${err.message}`, { stack: err.stack });
      });

    logger.debug('KafkaItemToMysqlJob pipeline running (unbounded stream).');
  }

  /** Stops consuming and flushes any buffered records. Not part of the original Java job, but useful for graceful shutdown. */
  async stop() {
    this._stopped = true;
    if (this._flushTimer) clearInterval(this._flushTimer);
    await this._flush();
    if (this._consumer) {
      await this._consumer.disconnect();
      this._consumer = null;
    }
  }

  async _flush() {
    if (this._buffer.length === 0) return;
    const batch = this._buffer.splice(0, this._buffer.length);
    await this._upsertBatch(batch);
  }

  async _upsertBatch(items) {
    const columns = ITEM_COLUMNS.map((c) => c.db);
    const placeholders = `(${columns.map(() => '?').join(', ')})`;
    const updateClause = ITEM_COLUMNS.filter((c) => !c.pk)
      .map((c) => `${c.db} = VALUES(${c.db})`)
      .join(', ');
    const sqlText = `INSERT INTO ${config.sinkDb.table} (${columns.join(', ')}) VALUES ${items
      .map(() => placeholders)
      .join(', ')} ON DUPLICATE KEY UPDATE ${updateClause}`;
    const values = items.flatMap((item) => item.toDbValues());

    let attempt = 0;
    while (attempt <= config.job.maxRetries) {
      try {
        await sinkPool.query(sqlText, values);
        logger.info(`Upserted batch of ${items.length} items into ${config.sinkDb.table}.`);
        return;
      } catch (err) {
        attempt++;
        logger.error(`Batch upsert attempt ${attempt} failed: ${err.message}`);
        if (attempt > config.job.maxRetries) {
          logger.error('Max retries exceeded for batch upsert; dropping batch to keep the stream alive.');
        } else {
          await new Promise((resolve) => setTimeout(resolve, 200 * attempt));
        }
      }
    }
  }
}

module.exports = KafkaItemToMysqlJob;

