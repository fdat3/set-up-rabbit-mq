const { createConnection } = require("../config/config.rabbitmq");
const jobService = require("../services/job.service");

class Consumer {
  constructor() {
    this.rabbit = createConnection("consumer");
    this.init();
  }

  async init() {
    this.rabbit.on("connection", async () => {
      console.log("Consumer connected to RabbitMQ");
      await this.rabbit.queueDeclare({ queue: "result-queue", durable: true });
      this.startConsumer();
    });
  }

  async startConsumer() {
    await this.rabbit.createConsumer(
      {
        queue: "job-queue",
        queueOptions: { durable: true },
        // qos: { prefetchCount: 1 }
      },
      async (msg) => {
        try {
          const job = msg.body;
          console.log(`Processing job ------- `);
          const result = await jobService.jobQueueMQ(job);

          const pub = await this.rabbit.createPublisher("result-queue", {
            properties: { deliveryMode: 2, contentType: "application/json" }
          });
          const resultPayload = {
            error: result.error,
            data: result.data,
            msg: result.msg
          };
          await pub.send("result-queue", resultPayload);
          console.log(`Sent result for job to result-queue ${resultPayload}`);
        } catch (error) {
          console.error("Error processing job:", error);
        }
      }
    );
  }

  async close() {
    await this.rabbit.close();
  }
}

module.exports = new Consumer();
