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
      await new Promise((resolve) => this.rabbit.once("connection", resolve)); // Chờ connection lần đầu
      console.log("Publisher connected to RabbitMQ");
      await this.rabbit.queueDeclare({ queue: "create_deal_queue", durable: true });
      this.channel = await this.rabbit.createPublisher("create_deal_queue", {
        properties: { deliveryMode: 2, contentType: "application/json" },
      });
      this.isInitialized = true;
      console.log("Publisher channel initialized");
    } catch (error) {
      console.error("Error initializing publisher:", error);
      throw error;
    }
  }

  async createDealQueue(deals) {
    try {
      const batchSize = 1000;
      let sequence = 0;
      this.channel = await this.rabbit.createPublisher("create_deal_queue", {
        properties: { deliveryMode: 2, contentType: "application/json" }
      });

      for (let i = 0; i < deals.length; i += batchSize) {
        const batch = deals.slice(i, i + batchSize).map((deal) => ({
          ...deal,
          sequence: sequence++
        }));
        this.channel.send(
          "create_deal_queue",
          batch
        );
        this.channel.ack()
        console.log(`Sent batch with sequence: ${batch[0].sequence}`);
      }
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