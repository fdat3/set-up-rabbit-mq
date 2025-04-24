/*eslint-disable */
/**
 * 
 * REQUIREMENT TRANG REPORT
 * 
 * ----> 1. Chuyển giao diện về FIT
 * ----> 2. Lưu data success/fail lên REDIS ( key: queueName:date )
 * ----> 3. Trang chi tiết show thêm Tên khách sạn - RoomTypeID - FromDateToDate ( Select SQL )
 * ----> 4. Sort theo queue actions gần nhất ( latest )
 */

const RabbitMQConfig = require("../../config/rabbitmq.config");
const $ = require("../../helpers/common_functions");
const { alertSystem } = require('../../services/webhook_services.js')
const REDISSERVICE = require('../../services/query_redis_service_cache.js');
const models = require("../../models/index.js");


class Publisher {
  constructor() {
    this.rabbit = new RabbitMQConfig();
    this.channel = null;
    this.isInitialized = false;
    this.messages = []; // Lưu message thành công
    this.failedMessages = []; // Lưu message thất bại
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

      await this.channel.assertExchange("dead_letter_exchange", "direct", {
        durable: true
      });
      await this.channel.assertQueue("dead_letter_queue", { durable: true });
      await this.channel.bindQueue(
        "dead_letter_queue",
        "dead_letter_exchange",
        "dead_letter_key"
      );

      const queueOptions = {
        durable: true,
        arguments: {
          "x-dead-letter-exchange": "dead_letter_exchange",
          "x-dead-letter-routing-key": "dead_letter_key"
        }
      };

      this.rabbit.setupQueue("update_deal_room_rate_queue", queueOptions);
      this.rabbit.setupQueue("create_deal_room_rate_queue", queueOptions);
      this.rabbit.setupQueue("create_deal_queue", queueOptions);
      this.rabbit.setupQueue("update_deal_queue", queueOptions);
      this.rabbit.setupQueue("update_multiple_room_queue", queueOptions);
      this.rabbit.setupQueue("update_multiple_info_room_queue", queueOptions);
      this.rabbit.setupQueue("deal_tracking_queue", { durable: true });

      console.log("Publisher connected to RabbitMQ");
      this.isInitialized = true;

      this.startConsumer("deal_tracking_queue");
    } catch (error) {
      console.error("Error initializing publisher:", error);
      return Promise.reject({ error: true, msg: error.message });
    }
  }

  async syncToRedis(trackingMessage) {
    try {
      if ($.isEmpty(trackingMessage)) {
        return { error: true, msg: "Missing value" };
      }
      const secondsToExpire = moment()
        .add(30, "days")
        .diff(moment(), "seconds");
      const now = new Date();
      let formattedDate = `${now.getFullYear()}-${String(
        now.getMonth() + 1
      ).padStart(2, 0)}-${now.getDate()}`;
      let hours = String(now.getHours()).padStart(2, "0");
      let minutes = String(now.getMinutes()).padStart(2, "0");
      let seconds = String(now.getSeconds()).padStart(2, "0");
      let keyRedisSearch = `deal_tracking_queue:${trackingMessage.queueName}:${formattedDate}`;
      let keyFormatDate = `${formattedDate} ${hours}:${minutes}:${seconds}`;
      const syncRedis = await REDISSERVICE("db3").pushRedis(
        keyRedisSearch,
        { [keyFormatDate]: trackingMessage.data },
        secondsToExpire,
        "hset"
      );
      return { error: false, data: syncRedis, msg: "Sync to redis success!" };
    } catch (error) {
      return { error: true, msg: error.message };
    }
  }

  async startConsumer(queueName) {
    try {
      const connection = await this.rabbit.connection;
      if (!this.channel) {
        this.channel = await connection.createChannel();
      }
      await this.channel.assertQueue(queueName, { durable: true });

      await this.channel.consume(
        queueName,
        (msg) => {
          if (msg) {
            try {
              const message = JSON.parse(msg.content.toString());
              console.log(`Received from ${queueName}:`, message);
              message.details.forEach((x) => {
                this.syncToRedis({ queueName: message.queueName, data: x });
                if (x.status === "failed") {
                  const logToGG = JSON.stringify({ ...message }, null, 2);
                  alertSystem(`::::Log RabbitMQ Error ${logToGG}`);
                }
              });
              this.channel.ack(msg);
            } catch (error) {
              console.error(`Error parsing message from ${queueName}:`, error);
              if (msg) {
                this.channel.nack(msg, false, false);
              }
            }
          }
        },
        { noAck: false }
      );
      return { error: false, msg: `Consumer started for ${queueName}` };
    } catch (error) {
      return {
        error: true,
        msg: `Error starting consumer for ${queueName}: ${error.message}`
      };
    }
  }

  async ensureQueue(queueName, options) {
    if ($.isEmpty(queueName)) {
      return { error: true, msg: "queueName not initialized" };
    }
    await this.channel.assertQueue(queueName, options);
    return { error: false, msg: "Assert queue success" };
  }

  async publishToQueue(queueName, dealObj, deals, batchSize) {
    try {
      if ($.isEmpty(queueName) || $.isEmpty(dealObj) || $.isEmpty(deals) || $.isEmpty(batchSize)) {
        return { error: true, msg: "Empty value" };
      }
      if (!this.isInitialized || !this.channel) {
        return { error: true, message: "Publisher not initialized" };
      }

      const queueOptions = {
        durable: true,
        arguments: {
          "x-dead-letter-exchange": "dead_letter_exchange",
          "x-dead-letter-routing-key": "dead_letter_key"
        }
      };
      await this.ensureQueue(queueName, queueOptions);

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
        console.log(
          `Sent batch to ${queueName} with sequence: ${batchSequence}`
        );
      }
      return { error: false, message: `Send message to ${queueName} success` };
    } catch (error) {
      console.error(`Error publishing to ${queueName}:`, error);
      return {
        error: true,
        msg: `Failed to publish to ${queueName}, ${error.message}`
      };
    }
  }

  async createDealRoomRateQueue(dealObj, deals) {
    if ($.isEmpty(dealObj) || $.isEmpty(deals)) {
      return { error: true, msg: "Empty value" };
    }
    return this.publishToQueue(
      "create_deal_room_rate_queue",
      dealObj,
      deals,
      100
    );
  }

  async updateDealRoomRateQueue(dealObj, deals) {
    if ($.isEmpty(dealObj) || $.isEmpty(deals)) {
      return { error: true, msg: "Empty value" };
    }
    return this.publishToQueue(
      "update_deal_room_rate_queue",
      dealObj,
      deals,
      100
    );
  }

  async updateMultipleRoomInfoQueue({ giaiDoanKhongApDung, arrRoomString, hotelId, arrRoom, priceOnWeb }) {
    if ($.isEmpty(giaiDoanKhongApDung) || !arrRoomString || !arrRoom) {
      return { error: true, msg: "Missing or invalid input" };
    }
  
    let sequence = 0;
    const batchSize = 5;
  
    try {
      for (let j = 0; j < giaiDoanKhongApDung.length; j += batchSize) {
        const batchSequence = sequence++;
        const giaiDoanBatch = giaiDoanKhongApDung.slice(j, j + batchSize);
  
        const batch = [
          {
            dealObj: {
              giaiDoan: giaiDoanBatch,
              roomtypeid: arrRoomString,
              arrRoom,
              hotelId,
              priceOnWeb
            },
            sequence: batchSequence,
          },
        ];
  
        const message = JSON.stringify(batch);
        console.log(`Sending batch ${batchSequence} with periods:`, giaiDoanBatch);
  
        await this.rabbit.sendMessage(
          "update_multiple_info_room_queue",
          message
        );
      }
  
      return { error: false, msg: "Sending to message success!" };
    } catch (error) {
      console.error("❌ Error sending to RabbitMQ:", error.message);
      return { error: true, msg: error.message };
    }
  }

  async updateMultipleRoomQueue(dealArr) {
    if ($.isEmpty(dealArr)) {
      return { error: true, msg: "Empty value" };
    }

    let sequence = 0;
    const batchSize = 10;
    try {
      console.log("Starting process...");
      console.log("dealArr length:", dealArr.length);

      for (let i = 0; i < dealArr.length; i++) {
        const deal = dealArr[i];
        const dealDates = deal.dealDate;
        console.log("dealDates length:", dealDates.length);
        console.log(
          "Expected batches:",
          Math.ceil(dealDates.length / batchSize)
        );

        for (let j = 0; j < dealDates.length; j += batchSize) {
          const batchSequence = sequence++;
          const dateBatch = dealDates.slice(j, j + batchSize);
          console.log(
            `Preparing batch ${batchSequence} with ${dateBatch.length} dates:`,
            dateBatch
          );

          const batch = [
            {
              stayDate: dateBatch,
              dealObj: { ...deal },
              sequence: batchSequence
            }
          ];
          const message = JSON.stringify(batch);
          console.log(`Before sending batch ${batchSequence}`);

          try {
            await this.rabbit.sendMessage(
              "update_multiple_room_queue",
              message
            );
            console.log(`Successfully sent batch ${batchSequence}`);
          } catch (error) {
            console.error(
              `Send error for batch ${batchSequence}:`,
              error.message
            );
            return { error: true, msg: error.message };
          }
        }
      }
      console.log("Process completed");
      return { error: false, msg: "Sending to message success!" };
    } catch (error) {
      return { error: true, msg: error.message };
    }
  }

  async updateDealQueue(dealObj) {
    try {
      if ($.isEmpty(dealObj)) {
        return { error: true, msg: "Empty value" };
      }
      if (!this.isInitialized || !this.channel) {
        return { error: true, msg: "Publisher not initialized" }
      }

      const data = [{ dealObj, sequence: 0 }];
      const message = JSON.stringify(data);
      await this.rabbit.sendMessage("update_deal_queue", message);
      return {
        error: false,
        message: "Send message to update_deal_queue success"
      };
    } catch (error) {
      console.error("Error in updateDealQueue:", error);
      return {
        error: true,
        msg: error.message
      };
    }
  }

  getUnprocessedQueues(initialQueue, result) {
    if($.isEmpty(initialQueue) || $.isEmpty(result)) {
      return [];
    }
    const processedQueueNames = result.map((item) => item.queueName);
    const unprocessedQueues = initialQueue.filter(
      (queueName) => !processedQueueNames.includes(queueName)
    );

    return unprocessedQueues;
  }

  async createDealQueue(dealObj) {
    try {
      if ($.isEmpty(dealObj)) {
        return { error: true, msg: "Empty value" };
      }
      const data = [{ dealObj, sequence: 0 }];
      const message = JSON.stringify(data);
      await this.rabbit.sendMessage("create_deal_queue", message);
      return {
        error: false,
        message: "Send message to create_deal_queue success"
      };
    } catch (error) {
      console.error("Error in createDealQueue:", error);
      return {
        error: true,
        msg: `Failed to send to create_deal_queue ${error.message}`
      };
    }
  }

  async trackingDealQueue(queueName, dateSelect) {
    try {
      if (!queueName) {
        return { error: true, msg: "Empty queueName" };
      }
      if (!this.isInitialized || !this.channel) {
        return { error: true, msg: "Publisher not initialized" };
      }

      const initialQueue = [
        "create_deal_queue",
        "create_deal_room_rate_queue",
        "update_deal_queue",
        "update_deal_room_rate_queue",
        "update_multiple_info_room_queue",
        "update_multiple_room_queue"
      ];

      let date = "";
      if ($.isEmpty(dateSelect)) {
        const now = new Date();
        date = `${now.getFullYear()}-${now.getMonth() + 1}-${now.getDate()}`;
      } else {
        date = dateSelect;
      }

      const getAllQueue = await REDISSERVICE("db3").scanAllKeyByLimit(
        `deal_tracking_queue:*:${date}`
      );
      const getUpdateQueueValue = await REDISSERVICE(
        "db3"
      ).hgetByKeysFullResults(getAllQueue);

      const groupedResult = {};

      getUpdateQueueValue.forEach((queueData, index) => {
        const queueKey = getAllQueue[index];
        const queueName = queueKey.split(":")[1];

        if (!groupedResult[queueName]) {
          groupedResult[queueName] = {
            queueName,
            successfulMessages: 0,
            failedMessages: 0
          };
        }

        Object.keys(queueData).forEach((timestamp) => {
          const parsedData = JSON.parse(queueData[timestamp]);
          const status = parsedData.status;

          if (status === "success") {
            groupedResult[queueName].successfulMessages += 1;
          } else {
            groupedResult[queueName].failedMessages += 1;
          }
        });
      });

      const result = Object.values(groupedResult);

      //filter cac queue chua chay
      const unprocessedQueues = this.getUnprocessedQueues(initialQueue, result);

      return {
        error: false,
        msg: "Queue processed successfully",
        data: {
          runningQueue: result,
          unRunningQueue: unprocessedQueues,
        }
      };
    } catch (error) {
      console.error("Error in trackingDealQueue:", error);
      return { error: true, msg: error.message };
    }
  }

  async getFailedMessages(queueName, dateSelect) {
    try {
      if (!queueName) {
        return { error: true, msg: "Empty queueName" };
      }
      if (!this.isInitialized || !this.channel) {
        return { error: true, msg: "Publisher not initialized" };
      }

      let date = "";
      if ($.isEmpty(dateSelect)) {
        const now = new Date();
        date = `${now.getFullYear()}-${now.getMonth() + 1}-${now.getDate()}`;
      } else {
        date = dateSelect;
      }

      const getDetailSuccessMessage = await REDISSERVICE("db3").hgetAllByKey(
        `deal_tracking_queue:${queueName}:${date}`
      );

      const getDateTime = Object.keys(getDetailSuccessMessage[0]);

      const dataResult = getDateTime.map((timestamp) => {
        const message = getDetailSuccessMessage[0][timestamp];
        let content = {};
        try {
          content = JSON.parse(message);
        } catch (parseError) {
          console.error(
            `Error parsing message for timestamp ${timestamp}:`,
            parseError
          );
        }

        return {
          originalQueue: queueName,
          content: content,
          timestamp: timestamp,
          error: content.error
        };
      });

      dataResult.sort((a, b) => {
        const dateA = new Date(
          a.timestamp.replace(/(\d+)-(\d+)-(\d+)/, "$1-$2-$3")
        );
        const dateB = new Date(
          b.timestamp.replace(/(\d+)-(\d+)-(\d+)/, "$1-$2-$3")
        );
        return dateB - dateA;
      });

      const filterSuccess = dataResult.filter(
        (item) => item.content.status === "failed"
      );

      return {
        error: false,
        msg: `Retrieved ${filterSuccess.length} failed messages for ${queueName}`,
        data: filterSuccess,
        totalFailed: filterSuccess.length
      };
    } catch (error) {
      console.error("Error in getFailedMessages:", error);
      return {
        error: true,
        msg: error.message
      };
    }
  }

  async getSuccessMessage(queueName, dateSelect) {
    try {
      if (!queueName) {
        return { error: true, msg: "Empty queueName" };
      }
      if (!this.isInitialized || !this.channel) {
        return { error: true, msg: "Publisher not initialized" };
      }

      let date = "";
      if ($.isEmpty(dateSelect)) {
        const now = new Date();
        date = `${now.getFullYear()}-${now.getMonth() + 1}-${now.getDate()}`;
      } else {
        date = dateSelect;
      }
      console.log("🚀 ~ Publisher ~ getSuccessMessage ~ date:", date);
      const getDetailSuccessMessage = await REDISSERVICE("db3").hgetAllByKey(
        `deal_tracking_queue:${queueName}:${date}`
      );

      const getDateTime = Object.keys(getDetailSuccessMessage[0]);

      const dataResult = getDateTime.map((timestamp) => {
        const message = getDetailSuccessMessage[0][timestamp];
        let content = {};
        try {
          content = JSON.parse(message);
          content.hotelId = content.data[0].dealObj.hotelId || null;
          content.roomtypeid = content.data[0].dealObj.roomtypeid.split(',').map(id => id.replace(/'/g, '').trim());
          content.fromDate = content.data[0].dealObj.fromDate || null;
          content.toDate = content.data[0].dealObj.toDate || null;
          content.donViGiaBan = content.data[0].dealObj.donViGiaBan || null;
          content.donViGiaMua = content.data[0].dealObj.donViGiaMua || null;
          content.giaBan = content.data[0].dealObj.giaBan;
          content.giaMua = content.data[0].dealObj.giaMua;
          content.priceOnWeb = content.data[0].dealObj.priceOnWeb || null;
          content.giaiDoan = JSON.stringify(content.data[0].dealObj.giaiDoan) || null;
          if(!$.isEmpty(content.data[0].dealObj.dataDealChange)) {
            const filterDate = content.data[0].dealObj.dataDealChange.filter((item) => item.date);
            content.dataDealChange = JSON.stringify(filterDate);
          }
        } catch (parseError) {
          console.error(
            `Error parsing message for timestamp ${timestamp}:`,
            parseError
          );
          content = { error: "Invalid message format", status: "failed" }; // Assume failed if parsing fails
        }

        return {
          originalQueue: queueName,
          content: content,
          timestamp: timestamp
        };
      });

      // Sort dataResult by timestamp (most recent first)
      dataResult.sort((a, b) => {
        const dateA = new Date(
          a.timestamp.replace(/(\d+)-(\d+)-(\d+)/, "$1-$2-$3")
        );
        const dateB = new Date(
          b.timestamp.replace(/(\d+)-(\d+)-(\d+)/, "$1-$2-$3")
        );
        return dateB - dateA; // Descending order (newest first)
      });

      // Filter for successful messages only
      const filterSuccess = dataResult.filter(
        (item) => item.content.status === "success"
      );

      return {
        error: false,
        msg: `Retrieved ${filterSuccess.length} success messages for ${queueName}`,
        data: filterSuccess,
        totalSuccess: filterSuccess.length
      };
    } catch (error) {
      console.error("Error in getSuccessMessage:", error);
      return {
        error: true,
        msg: error.message
      };
    }
  }

  async retryFailedMessage(queueName, timestamp, dateSelect) {
    try {
      if (
        $.isEmpty(queueName) ||
        $.isEmpty(timestamp) ||
        $.isEmpty(dateSelect)
      ) {
        return {
          error: true,
          msg: "Missing queueName or timestamp, dateSelect"
        };
      }
      if (!this.isInitialized || !this.channel) {
        return { error: true, msg: "Publisher not initialized" };
      }

      const getDataFromRedis = await REDISSERVICE("db3").hgetAllByKey(
        `deal_tracking_queue:${queueName}:${dateSelect}`
      );

      const getDateTime = Object.keys(getDataFromRedis[0]);

      const dataResult = getDateTime.map((timestampRedis) => {
        if (timestampRedis === timestamp) {
          return { error: false, data: getDataFromRedis[0][timestampRedis] };
        } else {
          return {
            error: false,
            data: []
          };
        }
      });

      if (!$.isEmpty(dataResult[0].data)) {
        const batch = this.createRetryBatch(queueName, dataResult);
        this.channel.sendToQueue(
          queueName,
          Buffer.from(JSON.stringify(batch)),
          { persistent: true }
        );
      }

      return {
        error: false,
        msg: `Message from ${queueName} at ${timestamp} in ${dateSelect} has been retried`
      };
    } catch (error) {
      console.error("Error retrying message:", error);
      return {
        error: true,
        msg: error.message
      };
    }
  }

  createRetryBatch(queueName, failedDetail) {
    const batch = failedDetail
      .filter((item) => !$.isEmpty(item.data))
      .map((item) => {
        let dataJson = {};
        dataJson = JSON.parse(item.data);

        const dealData = dataJson.data.deal.dealObj || dataJson.data.deal;
        const defaultSequence = 0;
        const queuesWithStayDate = [
          "create_deal_room_rate_queue",
          "update_deal_room_rate_queue",
          "update_multiple_room_queue"
        ];

        const batchItem = {
          dealObj: { ...dealData },
          sequence: defaultSequence
        };

        if (queuesWithStayDate.includes(queueName)) {
          batchItem.stayDate = dealData.stayDate;
        }

        return batchItem;
      })
      .filter((item) => item !== null);

    return batch;
  }

  async getHotelInfo(params) {
    try {
      if ($.isEmpty(params.hotelId) || $.isEmpty(params.roomTypeId)) {
        return { error: true, msg: "Missing value or invalid roomTypeId" };
      }
      
      const splitArr = params.roomTypeId[0].split(",").map((id) => id.trim());
      
      const getHotelInfo = models.Hotel.findOne({
        where: { id: params.hotelId },
        attributes: ["hotelname2"]
      });

      const getRoomTypeName = models.Roomtypeinfo.findAll({
        where: {
          roomtypeid: splitArr,
          lang: "vi-VN"
        },
        attributes: ["roomtypeid", "roomname", "lang"]
      });

      const [hotelInfo, roomTypeList] = await Promise.all([
        getHotelInfo,
        getRoomTypeName
      ]);

      if (!hotelInfo) {
        return { error: true, msg: "Hotel not found" };
      }

      const result = {
        hotelName: hotelInfo.hotelname2,
        roomNames: roomTypeList.map((rt) => ({
          roomTypeId: rt.roomtypeid,
          roomName: rt.roomname,
          lang: rt.lang
        }))
      };

      return {
        error: false,
        data: result
      };
    } catch (error) {
      return { error: true, msg: error.message, code: error.status || 500 };
    }
  }

  async close() {
    try {
      if (!$.isEmpty(this.channel)) {
        this.rabbit.close();
        this.channel = null;
        this.isInitialized = false;
        return { error: false, msg: "Publisher connection closed" };
      }
    } catch (error) {
      console.error("Error closing publisher:", error);
      return { error: true, msg: error.message };
    }
  }
}

const publisher = new Publisher();
module.exports = publisher;
