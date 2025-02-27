const { createConnection } = require("../config/config.rabbitmq");

class Publisher {
  constructor() {
    this.rabbit = createConnection("publisher");
    this.channel = null;
    this.init();
  }

  async init() {
    try {
      this.rabbit.on("connection", async () => {
        console.log("Publisher connected to RabbitMQ");
        await this.rabbit.queueDeclare({ queue: "job-queue", durable: true });
        this.channel = await this.rabbit.createPublisher("job-queue", {
          properties: { deliveryMode: 2, contentType: "application/json" },
        });
        console.log("Publisher channel initialized");
      });
    } catch (error) {
      console.error("Error initializing publisher:", error);
    }
  }

  async publishJob(job) {
    try {
      if (!this.channel) {
        console.log("Channel not ready, waiting for initialization...");
        await new Promise((resolve) => {
          this.rabbit.once("connection", resolve);
        });
        if (!this.channel) throw new Error("Channel initialization failed");
      }

      await this.channel.send("job-queue", job);
      return { error: false, message: "Job published successfully" };
    } catch (error) {
      console.error("Error publishing job:", error);
      return { error: true, message: "Failed to publish job" };
    }
  }

  async close() {
    if (this.channel) {
      await this.channel.close();
      this.channel = null;
    }
    await this.rabbit.close();
  }
}

module.exports = new Publisher();