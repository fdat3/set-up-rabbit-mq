const { createConnection } = require("../config/config.rabbitmq");

class Publisher {
  constructor() {
    this.rabbit = createConnection("publisher");
    this.channel = null;
    this.init();
  }

  async init() {
    this.rabbit.on("connection", async () => {
      console.log("Publisher connected to RabbitMQ");
      await this.rabbit.queueDeclare({ queue: "job-queue", durable: true });
      this.channel = await this.rabbit.acquire('job-channel');
    });
  }

  async publishJob(job) {
    try {
      if (!this.channel) await this.init();
      this.channel = await this.rabbit.createPublisher("job-queue", {
        properties: { deliveryMode: 2, contentType: "application/json" }
      });
      await this.channel.send("job-queue", job);
    } catch (error) {
      console.error("Error publishing job:", error);
      return { error: true, message: "Failed to publish job" };
    }
  }

  async close() {
    if (this.channel) await this.channel.close();
    await this.rabbit.close();
  }
}

module.exports = new Publisher();
