/* eslint-disable */

const RabbitMQConfig = require("../config/config.rabbitmq");

class Publisher {
  constructor() {
    this.rabbit = new RabbitMQConfig({
      host: "localhost",
      port: 5672,
      username: "guest",
      password: "guest"
    });
    this.channel = null;
    this.isInitialized = false;
    this.init();
  }

  async init() {
    try {
      await new Promise((resolve, reject) => {
        this.rabbit.connect((instance) => {
          if (instance) {
            this.channel = instance.channel;
            resolve();
          } else {
            reject({ error: true, msg: "Không thể tạo channel" });
          }
        });
      });

      // Khai báo Dead Letter Exchange và Queue
      this.channel.assertExchange('dead_letter_exchange', 'direct', { durable: true });
      this.channel.assertQueue('dead_letter_queue', { durable: true });
      this.channel.bindQueue('dead_letter_queue', 'dead_letter_exchange', 'dead_letter_key');

      // Cấu hình queue chính với DLQ
      this.rabbit.setupQueue("update_deal_queue", {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': 'dead_letter_exchange',
          'x-dead-letter-routing-key': 'dead_letter_key'
        }
      });
      this.rabbit.setupQueue("create_deal_queue", {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': 'dead_letter_exchange',
          'x-dead-letter-routing-key': 'dead_letter_key'
        }
      });
      this.rabbit.setupQueue("deal_tracking_queue", { durable: true });

      console.log("Publisher connected to RabbitMQ");
      this.isInitialized = true;
    } catch (error) {
      console.error("Error initializing publisher:", error);
      return Promise.reject({ error: true, msg: error.message });
    }
  }

  async publishToQueue(queueName, dealObj, deals, batchSize) {
    try {
      if (!this.isInitialized || !this.channel) {
        return { error: true, message: "Publisher not initialized" };
      }

      let sequence = 0;
      for (let i = 0; i < deals.length; i += batchSize) {
        const batchSequence = sequence++;
        const batch = deals.slice(i, i + batchSize).map((deal) => ({
          stayDate: deal,
          dealObj: { ...dealObj },
          sequence: batchSequence
        }));

        const message = JSON.stringify(batch);
        await this.rabbit.sendMessage(queueName, message);
        console.log(`Sent batch to ${queueName} with sequence: ${batchSequence}`);
      }
      return { error: false, message: `Send message to ${queueName} success` };
    } catch (error) {
      console.error(`Error publishing to ${queueName}:`, error);
      return { error: true, message: `Failed to publish to ${queueName}` };
    }
  }

  async createDealQueue(dealObj, deals) {
    return this.publishToQueue("create_deal_queue", dealObj, deals, 1000);
  }

  async updateDealQueue(dealObj, deals) {
    return this.publishToQueue("update_deal_queue", dealObj, deals, 1000);
  }

  async trackingDealQueue(queueName) {
    if (!this.channel) {
      return { error: true, msg: "Channel not initialized. Please connect first." };
    }
    this.channel.assertQueue(queueName, { durable: true });
    this.channel.consume(
      queueName,
      async (msg) => {
        try {
          if (msg) {
            const message = JSON.parse(msg.content.toString());
            console.log("deal_tracking_queue", message);
            this.channel.ack(msg);
            return { error: false, data: message };
          }
        } catch (error) {
          console.error(`Lỗi xử lý message từ ${queueName}:`, error);
          if (msg) {
            this.channel.nack(msg, false, true);
          }
        }
      },
      { noAck: false }
    );
    return {
      error: false,
      msg: `[xxx] Listening to messages from ${queueName}`
    };
  }

  async close() {
    try {
      if (this.channel) {
        await this.rabbit.close();
        this.channel = null;
        this.isInitialized = false;
        console.log("Publisher connection closed");
      }
    } catch (error) {
      console.error("Error closing publisher:", error);
      return { error: true, msg: error.message };
    }
  }
}

module.exports = new Publisher();