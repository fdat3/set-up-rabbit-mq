const { createConnection } = require("../config/config.rabbitmq");
const jobService = require("../services/job.service");
const fs = require("fs");

class Consumer {
  constructor() {
    this.rabbit = createConnection("consumer");
    this.resultChannel = null; // Channel cho result-queue
    this.isInitialized = false;
    this.init();
  }

  async init() {
    try {
      if (this.isInitialized) return;

      await new Promise((resolve) => this.rabbit.once("connection", resolve));
      console.log("Consumer connected to RabbitMQ");

      // Khởi tạo queue
      await this.rabbit.queueDeclare({ queue: "job-queue", durable: true });
      await this.rabbit.queueDeclare({ queue: "result-queue", durable: true });

      // Tạo channel cho result-queue một lần
      this.resultChannel = await this.rabbit.createPublisher("result-queue", {
        properties: { deliveryMode: 2, contentType: "application/json" },
      });

      this.isInitialized = true;
      this.startConsumer();
    } catch (error) {
      console.error("Error initializing consumer:", error);
      throw error;
    }
  }

  async startConsumer() {
    await this.rabbit.createConsumer(
      {
        queue: "job-queue",
        queueOptions: { durable: true },
        // qos: { prefetchCount: 1 } // Uncomment nếu cần giới hạn prefetch
      },
      async (msg) => {
        try {
          const job = msg.body;
          console.log(`Processing job ------- `);
          const result = await jobService.jobQueueMQ(job);
          const logLine = `Processed message with sequence: ${job.sequence_number} at ${new Date().toISOString()}\n`;
          fs.appendFileSync("log-queue.txt", logLine);

          const resultPayload = {
            error: result.error,
            data: result.data,
            msg: result.msg,
          };
          await this.resultChannel.send("result-queue", resultPayload); // Tái sử dụng channel
          console.log(`Sent result for job to result-queue ${JSON.stringify(resultPayload)}`);
        } catch (error) {
          console.error("Error processing job:", error);
        }
      }
    );
  }

  async close() {
    if (this.resultChannel) {
      await this.resultChannel.close();
      this.resultChannel = null;
    }
    await this.rabbit.close();
  }
}

module.exports = new Consumer();