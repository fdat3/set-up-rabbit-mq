const { getChannel, exchangeName } = require('../config/config.rabbitmq');
const { isSpamming } = require('./spam.service');

// Giả lập hàm lưu vào database (bạn có thể thay bằng PostgreSQL, MongoDB, v.v.)
const saveMessagesToDB = async (messages) => {
  console.log('[DB] Saving batch of messages:', messages);
};

async function consumeRoomMessages(io) {
  const channel = getChannel();
  await channel.assertQueue('chat.room.processor');
  await channel.bindQueue('chat.room.processor', exchangeName, 'chat.message.*');

  channel.consume('chat.room.processor', (msg) => {
    const data = JSON.parse(msg.content.toString());
    io.to(data.room).emit('chat-message', data);
    channel.ack(msg);
  });
}

// Xử lý log
async function consumeLogs() {
  const channel = getChannel();
  await channel.assertQueue('chat.logger');
  await channel.bindQueue('chat.logger', exchangeName, 'chat.log.*');

  channel.consume('chat.logger', (msg) => {
    const log = JSON.parse(msg.content.toString());
    console.log('[Log] Saved message:', log);
    channel.ack(msg);
  });
}

// Xử lý lưu vào database với batch size 100
async function consumeInsertMessage() {
  const channel = getChannel();
  await channel.assertQueue('chat.write');
  await channel.bindQueue('chat.write', exchangeName, 'chat.write');

  const BATCH_SIZE = 100;
  let messageBatch = [];

  channel.consume('chat.write', async (msg) => {
    const message = JSON.parse(msg.content.toString());
    messageBatch.push(message);

    // Dùng for để slice và xử lý ngay lập tức
    const totalMessages = messageBatch.length;
    for (let i = 0; i < totalMessages; i += BATCH_SIZE) {
      const batchSize = Math.min(BATCH_SIZE, totalMessages - i);
      const batchToProcess = messageBatch.slice(i, i + batchSize);

      try {
        await saveMessagesToDB(batchToProcess);
        console.log(`[DB] Inserted batch of ${batchToProcess.length} messages`);
      } catch (err) {
        console.error('[DB] Error saving batch:', err);
      }
    }

    // Reset messageBatch sau khi xử lý toàn bộ
    messageBatch = [];

    channel.ack(msg);
  }, { noAck: false });
}

// Xử lý gửi message từ client
function handleChatMessage(io, socket) {
  return async ({ room, content }) => {
    if (isSpamming(socket.id)) {
      socket.emit('spam-warning', 'Bạn đang gửi quá nhanh!');
      return;
    }

    const message = {
      room,
      user: socket.id,
      content,
      timestamp: new Date().toISOString(),
    };

    const channel = getChannel();
    if (!channel) {
      console.error('RabbitMQ channel chưa sẵn sàng!');
      return;
    }

    // Gửi message chính đến room
    channel.publish(exchangeName, `chat.message.${room}`, Buffer.from(JSON.stringify(message)));

    // Gửi log
    channel.publish(exchangeName, `chat.log.${room}`, Buffer.from(JSON.stringify(message)));

    // Gửi lưu DB
    channel.publish(exchangeName, `chat.write`, Buffer.from(JSON.stringify(message)));

    console.log('Published message to multiple routes via topic exchange');
  };
}

module.exports = { handleChatMessage, consumeInsertMessage, consumeLogs, consumeRoomMessages };