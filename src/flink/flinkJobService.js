const logger = require('../utils/logger');
const JobStatus = require('./jobStatus');
const MssqlItemToKafkaJob = require('./jobs/mssqlItemToKafkaJob');
const KafkaItemToMysqlJob = require('./jobs/kafkaItemToMysqlJob');
const FlinkWordStreamDemoJob = require('./jobs/flinkWordStreamDemoJob');

/**
 * Orchestrates the three streaming/batch jobs exposed by
 * flinkJobController.js and tracks their last-known JobStatus in-memory,
 * mirroring com.antontech.itemkafka_poc.service.FlinkJobService.
 */
class FlinkJobService {
  constructor() {
    /** @type {Map<string, string>} */
    this._jobStatuses = new Map();
    this._job2Instance = null;
  }

  /** Runs the source-DB -> Kafka batch job. Updates status for "Flink Job 1". */
  async runJob1() {
    await this._runWithLogging(async () => {
      const job = new MssqlItemToKafkaJob();
      await job.run();
    }, 'Flink Job 1');
  }

  /** Starts the Kafka -> MySQL unbounded streaming job. Updates status for "Flink Job 2". */
  async runJob2() {
    await this._runWithLogging(async () => {
      this._job2Instance = new KafkaItemToMysqlJob();
      await this._job2Instance.run();
    }, 'Flink Job 2');
  }

  /** Stops the currently-running Job 2 stream, if any. */
  async stopJob2() {
    if (this._job2Instance) {
      await this._job2Instance.stop();
      this._job2Instance = null;
      this._updateStatus('Flink Job 2', JobStatus.COMPLETED);
    }
  }

  /** Runs the lightweight, dependency-free demo job synchronously. Updates status for "Flink Simple Job". */
  async runSimpleJob() {
    await this._runWithLogging(async () => {
      FlinkWordStreamDemoJob.run();
    }, 'Flink Simple Job');
  }

  /** @param {string} jobName @returns {string} the last known JobStatus, defaulting to PENDING. */
  getJobStatus(jobName) {
    return this._jobStatuses.get(jobName) || JobStatus.PENDING;
  }

  async _runWithLogging(fn, jobName) {
    try {
      this._updateStatus(jobName, JobStatus.RUNNING);
      await fn();
      // Job 2 is an unbounded stream - once started successfully it should
      // stay RUNNING (there's no natural COMPLETED state), matching the
      // Java POC's documented behaviour.
      if (jobName !== 'Flink Job 2') {
        this._updateStatus(jobName, JobStatus.COMPLETED);
      }
    } catch (err) {
      this._updateStatus(jobName, JobStatus.FAILED);
      logger.error(`Execution failed for ${jobName}: ${err.message}`, { stack: err.stack });
    }
  }

  _updateStatus(jobName, status) {
    this._jobStatuses.set(jobName, status);
    logger.info(`Job ${jobName} is now ${status}`);
  }
}

module.exports = new FlinkJobService();

