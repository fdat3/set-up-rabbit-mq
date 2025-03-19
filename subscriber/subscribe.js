/* eslint-disable */

const RabbitMQConfig = require("../config/config.rabbitmq");
const uuid = require('node-uuid');
const models = require("../../models");

class Consumer {
  constructor() {
    this.rabbit = new RabbitMQConfig({
      host: 'localhost',
      port: 5672,
      username: 'guest',
      password: 'guest'
    });
    this.channel = null;
    this.isInitialized = false;
    this.init();
  }

  async init() {
    try {
      if (this.isInitialized) return;

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

      this.channel.prefetch(1);

      // this.channel.assertExchange('dead_letter_exchange', 'direct', { durable: true });
      // this.channel.assertQueue('dead_letter_queue', { durable: true });
      // this.channel.bindQueue('dead_letter_queue', 'dead_letter_exchange', 'dead_letter_key');

      console.log("Consumer connected to RabbitMQ");
      this.isInitialized = true;
      this.consumeQueue('update_deal_queue', this.handleUpdateDeal.bind(this));
      this.consumeQueue('create_deal_queue', this.handleCreateDeal.bind(this));
    } catch (error) {
      console.error("Error initializing consumer:", error);
      return Promise.reject({ error: true, msg: error.message });
    }
  }

  consumeQueue(queueName, handler) {
    console.log(`Bắt đầu lắng nghe trên ${queueName}...`);
    this.channel.assertQueue(queueName, {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': 'dead_letter_exchange',
        'x-dead-letter-routing-key': 'dead_letter_key'
      }
    });

    this.channel.consume(queueName, async (msg) => {
      console.log(`🚀 ~ Consumer ~ Nhận msg từ ${queueName}:`, msg);
      try {
        if (msg) {
          const batch = JSON.parse(msg.content.toString());
          console.log(`🚀 ~ Consumer ~ Batch từ ${queueName}:`, batch);
          const batchSequence = batch[0].sequence;

          const result = await handler(batch);
          const hasFailed = result.data.details.some(deal => deal.status === "failed");

          if (hasFailed) {
            console.log(`🚀 ~ Có deal thất bại trong batch từ ${queueName}, gửi tới DLQ`);
            this.channel.nack(msg, false, false); // Gửi tới DLQ
          } else {
            console.log(`Processed batch từ ${queueName} với sequence: ${batchSequence}`);
            this.channel.ack(msg); // Xác nhận thành công
          }
        }
      } catch (error) {
        console.error(`Lỗi xử lý job từ ${queueName}:`, error);
        if (msg) {
          this.channel.nack(msg, false, false); // Gửi tới DLQ nếu lỗi tổng quát
        }
      }
    }, { noAck: false });
    return {
      error: false,
      msg: `Consumer started listening on ${queueName}`
    };
  }

  async handleUpdateDeal(batch) {
    const batchResults = [];
    for (let i = 0; i < batch.length; i++) {
      const deal = batch[i];
      console.log(`🚀 ~ Consumer ~ Cập nhật deal (vị trí ${i}):`, deal);
      try {
        await models.Deal.update(
          {
            baseratebeforetax: deal.dealObj.baseratebeforetax,
            baserateaftertax: deal.dealObj.baserateaftertax,
            baserateaftertaxvnd: deal.dealObj.baserateaftertax,
            baseratewholesale: deal.dealObj.baseratewholesale,
            baseratewholesalevnd: deal.dealObj.baseratewholesalevnd,
            pobaserate: deal.dealObj.pobaserate,
            pobaseratevnd: deal.dealObj.pobaseratevnd,
            commission: deal.dealObj.commission,
            commissionvnd: deal.dealObj.commissionvnd,
            commissionpercentage: deal.dealObj.commissionpercentage
          },
          {
            where: {
              $and: [
                { roomtypeid: deal.dealObj.roomtypeid },
                models.sequelize.where(
                  models.sequelize.fn("DATE_FORMAT", models.sequelize.col("dealdate"), "%Y-%m-%d"),
                  "=",
                  deal.stayDate
                )
              ]
            }
          }
        );
        batchResults.push({
          index: i,
          data: {
            baseratebeforetax: deal.dealObj.baseratebeforetax,
            baserateaftertax: deal.dealObj.baserateaftertax,
            baserateaftertaxvnd: deal.dealObj.baserateaftertax,
            baseratewholesale: deal.dealObj.baseratewholesale,
            baseratewholesalevnd: deal.dealObj.baseratewholesalevnd,
            pobaserate: deal.dealObj.pobaserate,
            pobaseratevnd: deal.dealObj.pobaseratevnd,
            commission: deal.dealObj.commission,
            commissionvnd: deal.dealObj.commissionvnd,
            commissionpercentage: deal.dealObj.commissionpercentage,
            stayDate: deal.stayDate
          },
          status: "success",
          error: false
        });
      } catch (err) {
        console.error(`🚀 ~ Lỗi khi cập nhật deal (vị trí ${i}):`, { deal, error: err.message });
        batchResults.push({
          index: i,
          data: {
            baseratebeforetax: deal.dealObj.baseratebeforetax,
            baserateaftertax: deal.dealObj.baserateaftertax,
            baserateaftertaxvnd: deal.dealObj.baserateaftertax,
            baseratewholesale: deal.dealObj.baseratewholesale,
            baseratewholesalevnd: deal.dealObj.baseratewholesalevnd,
            pobaserate: deal.dealObj.pobaserate,
            pobaseratevnd: deal.dealObj.pobaseratevnd,
            commission: deal.dealObj.commission,
            commissionvnd: deal.dealObj.commissionvnd,
            commissionpercentage: deal.dealObj.commissionpercentage,
            stayDate: deal.stayDate
          },
          status: "failed",
          error: err.message
        });
      }
    }
    const result = await this.sendToTrackingQueue('update_deal_queue', batchResults);
    return { error: false, data: result };
  }

  async handleCreateDeal(batch) {
    const batchResults = [];
    for (let i = 0; i < batch.length; i++) {
      const deal = batch[i];
      console.log(`🚀 ~ Consumer ~ Tạo deal (vị trí ${i}):`, deal);
      try {
        const newDeal = await models.Deal.create({
          id: uuid.v1().toUpperCase(),
          roomtypeid: deal.dealObj.roomtypeid,
          dealdate: new Date(deal.stayDate),
          baseratebeforetax: deal.dealObj.baseratebeforetax,
          baserateaftertax: deal.dealObj.baserateaftertax,
          baserateaftertaxvnd: deal.dealObj.baserateaftertaxvnd,
          baseratewholesale: deal.dealObj.baseratewholesale,
          baseratewholesalevnd: deal.dealObj.baseratewholesalevnd,
          pobaserate: deal.dealObj.pobaserate,
          pobaseratevnd: deal.dealObj.pobaseratevnd,
          commission: deal.dealObj.commission,
          commissionvnd: deal.dealObj.commissionvnd,
          commissionpercentage: deal.dealObj.commissionpercentage
        });
        batchResults.push({
          index: i,
          data: { newDeal },
          status: "success",
          error: null
        });
      } catch (err) {
        console.error(`🚀 ~ Lỗi khi tạo deal (vị trí ${i}):`, { deal, error: err.message });
        batchResults.push({
          index: i,
          data: {
            roomtypeid: deal.dealObj.roomtypeid,
            dealdate: new Date(deal.stayDate),
            baseratebeforetax: deal.dealObj.baseratebeforetax,
            baserateaftertax: deal.dealObj.baserateaftertax,
            baserateaftertaxvnd: deal.dealObj.baserateaftertaxvnd,
            baseratewholesale: deal.dealObj.baseratewholesale,
            baseratewholesalevnd: deal.dealObj.baseratewholesalevnd,
            pobaserate: deal.dealObj.pobaserate,
            pobaseratevnd: deal.dealObj.pobaseratevnd,
            commission: deal.dealObj.commission,
            commissionvnd: deal.dealObj.commissionvnd,
            commissionpercentage: deal.dealObj.commissionpercentage
          },
          status: "failed",
          error: err.message
        });
      }
    }
    const result = await this.sendToTrackingQueue("create_deal_queue", batchResults);
    return { error: false, data: result };
  }

  async sendToTrackingQueue(queueName, batchResults) {
    const trackingMessage = {
      queueName,
      timestamp: new Date().toISOString(),
      details: batchResults // Gửi cả index trong details
    };
    this.rabbit.sendMessage(
      'deal_tracking_queue',
      Buffer.from(JSON.stringify(trackingMessage))
    );
    return {
      error: false,
      msg: 'Đã gửi thông tin tới deal_tracking_queue.',
      details: batchResults
    };
  }

  async close() {
    try {
      if (this.channel) {
        await this.rabbit.close();
        this.channel = null;
        this.isInitialized = false;
        return { error: false, msg: "Consumer connection closed" };
      }
    } catch (error) {
      console.error("Error closing consumer:", error);
      return { error: false, msg: error.message };
    }
  }
}

module.exports = new Consumer();