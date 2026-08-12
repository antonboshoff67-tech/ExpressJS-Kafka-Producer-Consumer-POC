const logger = require('../../utils/logger');

/**
 * Dependency-free smoke-test job, mirrors FlinkWordStreamDemoJob. Validates
 * the "streaming job runtime" independent of Kafka/JDBC by running a tiny
 * in-memory word-count stream over a fixed sentence list. Runs synchronously
 * and quickly, so it can be awaited directly by the controller.
 * <p>
 * Kick off via POST /flink/start-simple-job.
 */
class FlinkWordStreamDemoJob {
  static run() {
    const sentences = [
      'to be or not to be',
      'that is the question',
      'to be or not to be that is the question',
    ];

    const counts = new Map();
    for (const sentence of sentences) {
      for (const word of sentence.split(/\s+/)) {
        counts.set(word, (counts.get(word) || 0) + 1);
      }
    }

    const result = Object.fromEntries(counts);
    logger.debug(`FlinkWordStreamDemoJob word counts: ${JSON.stringify(result)}`);
    return result;
  }
}

module.exports = FlinkWordStreamDemoJob;

