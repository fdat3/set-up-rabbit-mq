const { createConnection } = require("../config/config.rabbitmq");

class Publisher {
  constructor() {
    this.rabbit = createConnection("publisher");
    this.channel = null;
    this.isInitialized = false;
    this.init();
  }

  async init() {
    try {
      if (this.isInitialized) return; // Ngăn khởi tạo lại
      await new Promise((resolve) => this.rabbit.once("connection", resolve)); // Chờ connection lần đầu
      console.log("Publisher connected to RabbitMQ");
      await this.rabbit.queueDeclare({ queue: "job-queue", durable: true });
      this.channel = await this.rabbit.createPublisher("job-queue", {
        properties: { deliveryMode: 2, contentType: "application/json" },
      });
      this.isInitialized = true;
      console.log("Publisher channel initialized");
    } catch (error) {
      console.error("Error initializing publisher:", error);
      throw error;
    }
  }

  async publishJob(job) {
    try {
      if (!this.isInitialized) {
        console.log("Channel not ready, waiting for initialization...");
        await this.init(); // Đảm bảo init hoàn tất
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