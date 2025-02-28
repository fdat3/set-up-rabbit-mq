const jobQueueMQ = async (job) => {
  try {
    // const result = await new Promise((resolve) => {
    //   setTimeout(() => {
    //     resolve({
    //       error: false,
    //       data: { result: `Processed ${job.id}` },
    //       code: 200,
    //       msg: 'Job processed successfully from service'
    //     });
    //   }, 1000); // 1 giây
    // });

    return {
      error: false,
      data: { result: `Processed ${job.sequence_number}` },
      code: 200,
      msg: "Job processed successfully from service"
    };
  } catch (error) {
    return {
      error: true,
      data: null,
      code: 500,
      msg: `Failed to process job: ${error.message}`
    };
  }
};

module.exports = { jobQueueMQ };
