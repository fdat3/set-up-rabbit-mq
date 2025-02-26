const jobQueueMQ = async (job) => {
  try {
    return {
      error: false,
      data: { result: `Processed ${job.id}` },
      code: 200,
      msg: 'Job processed successfully from service'
    };
  } catch (error) {
    return {
      error: true,
      data: null,
      code: 500,
      msg: 'Failed to process job'
    };
  }
};

module.exports = { jobQueueMQ };