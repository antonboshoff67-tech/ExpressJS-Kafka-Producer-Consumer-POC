const express = require('express');
const router = express.Router();
const flinkJobService = require('../flink/flinkJobService');
const logger = require('../utils/logger');

/**
 * @openapi
 * /flink/start-job1:
 *   post:
 *     summary: Trigger the source-DB -> Kafka batch job asynchronously
 *     tags: [Flink Jobs]
 *     responses:
 *       200: { description: Job submitted }
 */
router.post('/start-job1', (req, res) => {
  // Fire-and-forget, mirrors CompletableFuture.runAsync(...) in the Java controller.
  flinkJobService.runJob1().catch((err) => logger.error(`Error starting Job 1: ${err.message}`));
  res.send('Flink Job 1 started successfully.');
});

/**
 * @openapi
 * /flink/start-job2:
 *   post:
 *     summary: Trigger the Kafka -> MySQL unbounded streaming job asynchronously
 *     tags: [Flink Jobs]
 *     responses:
 *       200: { description: Job submitted }
 */
router.post('/start-job2', (req, res) => {
  flinkJobService.runJob2().catch((err) => logger.error(`Error starting Job 2: ${err.message}`));
  res.send('Flink Job 2 started successfully.');
});

/**
 * @openapi
 * /flink/stop-job2:
 *   post:
 *     summary: Stop the currently-running Kafka -> MySQL streaming job
 *     tags: [Flink Jobs]
 *     responses:
 *       200: { description: Job stopped }
 */
router.post('/stop-job2', async (req, res) => {
  try {
    await flinkJobService.stopJob2();
    res.send('Flink Job 2 stopped.');
  } catch (err) {
    logger.error(`Error stopping Job 2: ${err.message}`);
    res.status(500).send('Error stopping Flink Job 2');
  }
});

/**
 * @openapi
 * /flink/start-simple-job:
 *   post:
 *     summary: Run the lightweight, dependency-free demo job synchronously
 *     tags: [Flink Jobs]
 *     responses:
 *       200: { description: Demo job executed successfully }
 *       400: { description: Error starting demo job }
 */
router.post('/start-simple-job', async (req, res) => {
  try {
    await flinkJobService.runSimpleJob();
    res.send('Flink Simple Job executed successfully.');
  } catch (err) {
    logger.error(`Error starting Simple Job: ${err.message}`);
    res.status(400).send('Error starting Simple Job');
  }
});

/**
 * @openapi
 * /flink/job-status:
 *   get:
 *     summary: Get the last known status for a job
 *     tags: [Flink Jobs]
 *     parameters:
 *       - in: query
 *         name: jobName
 *         schema: { type: string, example: "Flink Job 1" }
 *     responses:
 *       200: { description: PENDING | RUNNING | COMPLETED | FAILED }
 */
router.get('/job-status', (req, res) => {
  const jobName = req.query.jobName;
  const status = flinkJobService.getJobStatus(jobName);
  logger.debug(`Job ${jobName} status requested: ${status}`);
  res.json(status);
});

module.exports = router;

