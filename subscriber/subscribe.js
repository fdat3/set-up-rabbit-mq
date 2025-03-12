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
      await this.rabbit.queueDeclare({ queue: "result-queue", durable: true });
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
        queue: "create_deal_queue",
        queueOptions: { durable: true },
        qos: { prefetchCount: 10 },
      },
      async (msg) => {
        try {
          const batch = msg.body;
          const batchSequence = batch[0].sequence;

          for (const deal of batch) {
            await jobService.jobQueueMQ(deal);
          }
          console.log(
            `Processed batch with sequence: ${batchSequence}`
          );
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