/*eslint-disable */

const RabbitMQConfig = require("../../config/rabbitmq.config");
const uuid = require("node-uuid");
const $ = require("../../helpers/common_functions");
const models = require("../../models");
var moment = require("moment");

class Consumer {
  constructor() {
    this.rabbit = new RabbitMQConfig();
    this.channel = null;
    this.isInitialized = false;
    this.init();
  }

  async init() {
    try {
      if (this.isInitialized) {
        return;
      }

      await new Promise((resolve, reject) => {
        this.rabbit.connect((instance) => {
          if (instance) {
            this.channel = instance.channel;
            resolve({ error: true, msg: "Create Channel success" });
          } else {
            reject({ error: true, msg: "Không thể tạo channel" });
          }
        });
      });

      this.channel.prefetch(10);
      console.log("Consumer connected to RabbitMQ");
      this.isInitialized = true;
      this.consumeQueue(
        "update_deal_room_rate_queue",
        this.handleUpdateDealRoomRate.bind(this)
      );
      this.consumeQueue(
        "create_deal_room_rate_queue",
        this.handleCreateDealRoomRate.bind(this)
      );
      this.consumeQueue("create_deal_queue", this.handleCreateQueue.bind(this));
      this.consumeQueue("update_deal_queue", this.handleUpdateDeal.bind(this));
      this.consumeQueue(
        "update_multiple_room_queue",
        this.handleUpdateMultipleRoom.bind(this)
      );
      this.consumeQueue(
        "update_multiple_info_room_queue",
        this.handleUpdateMultipleInfoRoom.bind(this)
      );
    } catch (error) {
      console.error("Error initializing consumer:", error);
      return { error: true, msg: error.message };
    }
  }

  consumeQueue(queueName, handler) {
    if ($.isEmpty(queueName) || $.isEmpty(handler)) {
      return { error: true, msg: "Missing value" };
    }
  
    this.channel.assertQueue(queueName, {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": "dead_letter_exchange",
        "x-dead-letter-routing-key": "dead_letter_key"
      }
    });
  
    this.channel.consume(
      queueName,
      async (msg) => {
        console.log(`🚀 ~ Consumer ~ Nhận msg từ ${queueName}:`, msg);
        try {
          if (msg) {
            const batch = JSON.parse(msg.content.toString());
            console.log(`🚀 ~ Consumer ~ Batch từ ${queueName}:`, batch);
            const batchSequence = batch[0].sequence ? batch[0].sequence : 0;
  
            const result = await handler(batch);
            console.log("🚀 ~ Consumer ~ result:", result);
            const hasFailed = result.data.details.some(
              (deal) => deal.status === "failed"
            );
            if (hasFailed) {
              console.log(
                `🚀 ~ Có deal thất bại trong batch từ ${queueName}, gửi tới DLQ`
              );
              this.channel.nack(msg, false, false);
            } else {
              console.log(
                `Processed batch từ ${queueName} với sequence: ${batchSequence}`
              );
              this.channel.ack(msg);
            }
          }
        } catch (error) {
          console.error(`Lỗi xử lý message từ ${queueName}:`, error);
          this.channel.nack(msg, false, false);
          return { error: true, msg: `Lỗi xử lý message từ ${queueName}` };
        }
      },
      { noAck: false }
    );
    return { error: false, msg: `Consumer started listening on ${queueName}` };
  }

  async handleUpdateDealRoomRate(batch) {
    if ($.isEmpty(batch)) {
      return { error: true, msg: "Missing value" };
    }
    const batchResults = [];
    const failedResults = [];
  
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
                  models.sequelize.fn(
                    "DATE_FORMAT",
                    models.sequelize.col("dealdate"),
                    "%Y-%m-%d"
                  ),
                  "=",
                  deal.stayDate
                )
              ]
            }
          }
        );
      } catch (err) {
        console.error(`🚀 ~ Lỗi khi cập nhật deal (vị trí ${i}):`, {
          deal,
          error: err.message
        });
        failedResults.push({
          index: i,
          data: { deal },
          status: "failed",
          error: err.message
        });
      }
    }
  
    if (failedResults.length === 0) {
      batchResults.push({
        status: "success",
        data: batch,
        total: batch.length
      });
    } else {
      batchResults.push(...failedResults);
    }
  
    const result = await this.sendToTrackingQueue(
      "update_deal_room_rate_queue",
      batchResults
    );
    return { error: false, data: result };
  }

  async handleUpdateDeal(batch) {
    if ($.isEmpty(batch)) {
      return { error: true, msg: "Missing value" };
    }
    const batchResults = [];
    const failResults = [];

    for (let i = 0; i < batch.length; i++) {
      const deal = batch[i];
      console.log(`🚀 ~ Consumer ~ Cập nhật deal (vị trí ${i}):`, deal);
      try {
        await models.Deal.update(
          {
            baseratebeforetax: deal.dealObj.baseratebeforetax,
            baserateaftertax: deal.dealObj.baserateaftertax,
            baserateaftertaxvnd: deal.dealObj.baserateaftertaxvnd,
            baseratewholesale: deal.dealObj.baseratewholesale,
            baseratewholesalevnd: deal.dealObj.baseratewholesalevnd,
            pobaserate: deal.dealObj.pobaserate,
            pobaseratevnd: deal.dealObj.pobaseratevnd,
            commission: deal.dealObj.commission,
            commissionnnd: deal.dealObj.commissionnnd,
            commissionpercentage2: deal.dealObj.commissionpercentage2
          },
          {
            where: { id: deal.dealObj.dealId }
          }
        );
      } catch (err) {
        console.error(`🚀 ~ Lỗi khi cập nhật deal (vị trí ${i}):`, {
          deal,
          error: err.message
        });
        failResults.push({
          index: i,
          data: { deal },
          status: "failed",
          error: err.message
        });
      }
    }
    if($.isEmpty(failResults)) {
      batchResults.push({
        data: [ batch[0] ],
        status: "success"
      });
    } else {
      batchResults.push(...failResults);
    }

    const result = await this.sendToTrackingQueue(
      "update_deal_queue",
      batchResults
    );
    return { error: false, data: result };
  }

  async handleCreateQueue(batch) {
    if ($.isEmpty(batch)) {
      return { error: true, msg: "Missing value" };
    }
    const batchResults = [];
    const failedMessage = [];

    for (let i = 0; i < batch.length; i++) {
      const deal = batch[i];
      console.log(`🚀 ~ Consumer ~ Tạo deal (vị trí ${i}):`, deal);
      try {
        const newDeal = await models.Deal.create({
          id: uuid.v1().toUpperCase(),
          baseratebeforetax: deal.dealObj.baseratebeforetax,
          baserateaftertax: deal.dealObj.baserateaftertax,
          baserateaftertaxvnd: deal.dealObj.baserateaftertaxvnd,
          baseratewholesale: deal.dealObj.baseratewholesale,
          baseratewholesalevnd: deal.dealObj.baseratewholesalevnd,
          pobaserate: deal.dealObj.pobaserate,
          pobaseratevnd: deal.dealObj.pobaseratevnd,
          commission: deal.dealObj.commission,
          commissionnnd: deal.dealObj.commissionnnd,
          commissionpercentage2: deal.dealObj.commissionpercentage2
        });
      } catch (err) {
        console.error(`🚀 ~ Lỗi khi tạo deal (vị trí ${i}):`, {
          deal,
          error: err.message
        });
        failedMessage.push({
          index: i,
          data: { deal },
          status: "failed",
          error: err.message
        });
      }
    }

    if($.isEmpty(failedMessage)) {
      batchResults.push({
        data: [ batch[0] ],
        status: "success",
        error: false
      });
    } else {
      batchResults.push(...failedMessage)
    }

    const result = await this.sendToTrackingQueue(
      "create_deal_queue",
      batchResults
    );
    return { error: false, data: result };
  }


  buildDateUpdateDeals(arrDate) {
    let stringDate = '';
    arrDate.forEach((item,index) => {
      if (index ==0) {
        stringDate += ` and (dealdate >='${this.checkDateRequest(item.from)}' and dealdate <='${this.checkDateRequest(item.to)} 23:59:59' `;
      } else {
        stringDate += ` || dealdate >='${this.checkDateRequest(item.from)}' and dealdate <='${this.checkDateRequest(item.to)} 23:59:59' `;
      }
    });
  
    if (stringDate.length > 10 ) {
      stringDate += ')';
    }
  
    return stringDate;
  };


  checkDateRequest(dateRequest) {
    if (!moment(dateRequest,'YYYY-MM-DD',true).isValid()) {
      dateRequest = moment(dateRequest,'DD/MM/YYYY').format("YYYY-MM-DD");
    }
  
    return dateRequest;
  }

  async handleUpdateMultipleInfoRoom(batch) {
    if ($.isEmpty(batch)) {
      return { error: true, msg: "Missing value" };
    }
    const { dealObj, arrRoomString, sequence } = batch[0];
    const batchResults = [];
    const failedMessage = [];

    let dealdateString = this.buildDateUpdateDeals(dealObj.giaiDoan);
    let strUpdateDeal = '';
    strUpdateDeal += `update deals set is_khong_ap_dung = false where roomtypeid in (${dealObj.roomtypeid}) AND dealdate >= '${moment().format('YYYY-MM-DD')}' AND is_khong_ap_dung = true;` ;
    strUpdateDeal += `update deals set is_khong_ap_dung = TRUE where roomtypeid in (${dealObj.roomtypeid}) ${dealdateString};`;
                
    try {
      await models.sequelize.query(strUpdateDeal);
    } catch (err) {
      console.error("Error updating rooms:", err);
      failedMessage.push({
        index: sequence,
        data: batch[sequence],
        status: "failed",
        error: err.message,
      });
    }

    if($.isEmpty(failedMessage)) {
      batchResults.push({
        index: sequence,
        data: [ batch[0] ],
        status: "success",
        error: false,
      });
    } else {
      batchResults.push(...failedMessage);
    }

    const result = await this.sendToTrackingQueue(
      "update_multiple_info_room_queue",
      batchResults
    );
    return { error: false, data: result };
  }

  async handleUpdateMultipleRoom(batch) {
    if ($.isEmpty(batch)) {
      return { error: true, msg: "Missing value" };
    }
    const { stayDate, dealObj, sequence } = batch[0];
    const batchResults = [];
    const failedMessage = [];
    const {
      pobaserate,
      pobaseratevnd,
      baserateaftertax,
      baserateaftertaxvnd,
      roomTypeId,
      arrRoom,
      itemGiaiDoanFrom,
      itemGiaiDoanTo
    } = dealObj;

    const strQuery = `
        UPDATE deals 
        SET 
            pobaserate = ${pobaserate},
            pobaseratevnd = ${pobaseratevnd},
            baserateaftertax = ${baserateaftertax},
            baserateaftertaxvnd = ${baserateaftertaxvnd}
        WHERE
            roomtypeid IN (${arrRoom})
            AND dealdate >= '${itemGiaiDoanFrom}'
            AND dealdate <= '${itemGiaiDoanTo} 23:59:59'
            AND DATE_FORMAT(dealdate, '%Y-%m-%d') IN ('${stayDate.join(
              "','"
            )}');
    `;
    console.log(
      "🚀 ~ Consumer ~ handleUpdateMultipleRoom ~ strQuery:",
      strQuery
    );
    try {
      await models.sequelize.query(strQuery);
    } catch (err) {
      console.error("Error updating rooms:", err);
      failedMessage.push({
        index: sequence,
        data: batch,
        status: "failed",
        error: err.message 
      });
    }

    if($.isEmpty(failedMessage)) {
      batchResults.push({
        data: [ batch[0] ],
        status: "success",
        error: false
      })
    } else {
      batchResults.push(...failedMessage)
    }

    const result = await this.sendToTrackingQueue(
      "update_multiple_room_queue",
      batchResults
    );
    return { error: false, data: result };
  }

  async handleCreateDealRoomRate(batch) {
    if ($.isEmpty(batch)) {
      return { error: true, msg: "Missing value" };
    }
    const batchResults = [];
    const failedMessage = [];
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
      } catch (err) {
        console.error(`🚀 ~ Lỗi khi tạo deal (vị trí ${i}):`, {
          deal,
          error: err.message
        });
        failedMessage.push({
          index: i,
          data: { deal },
          status: "failed",
          error: err.message
        });
      }
    }
    if($.isEmpty(failedMessage)) {
      batchResults.push({
        data: [ batch[0] ],
        status: "success",
        error: false
      });
    } else {
      batchResults.push(...failedMessage);
    }

    const result = await this.sendToTrackingQueue(
      "create_deal_room_rate_queue",
      batchResults
    );
    return { error: false, data: result };
  }

  async sendToTrackingQueue(queueName, batchResults) {
    try {
      if ($.isEmpty(queueName) || $.isEmpty(batchResults)) {
        return { error: true, msg: "Missing value" };
      }
      const trackingMessage = {
        queueName,
        timestamp: new Date().toISOString(),
        details: batchResults
      };
      this.rabbit.sendMessage(
        "deal_tracking_queue",
        Buffer.from(JSON.stringify(trackingMessage))
      );
      return {
        error: false,
        msg: "Đã gửi thông tin tới deal_tracking_queue.",
        details: batchResults
      };
    } catch (error) {
      return { error: true, msg: error.message };
    }
  }

  async close() {
    try {
      if (!$.isEmpty(this.channel)) {
        await this.rabbit.close();
        this.channel = null;
        this.isInitialized = false;
        return { error: false, msg: "Consumer connection closed" };
      }
    } catch (error) {
      console.error("Error closing consumer:", error);
      return { error: true, msg: error.message };
    }
  }
}

module.exports = new Consumer();
