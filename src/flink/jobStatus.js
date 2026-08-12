/** Enum-like job status values, mirrors com.antontech.itemkafka_poc.service.JobStatus. */
const JobStatus = Object.freeze({
  PENDING: 'PENDING',
  RUNNING: 'RUNNING',
  COMPLETED: 'COMPLETED',
  FAILED: 'FAILED',
});

module.exports = JobStatus;

