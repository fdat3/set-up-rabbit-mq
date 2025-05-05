const { getChannel, exchangeName } = require('../config/config.rabbitmq');
const { isSpamming } = require('./spam.service');
async function consumeRoomMessages(io) {
  const channel = getChannel();
  await channel.assertQueue('chat.room.processor');
  await channel.bindQueue('chat.room.processor', exchangeName, 'chat.message.*');

  channel.consume('chat.room.processor', (msg) => {
    const data = JSON.parse(msg.content.toString());
    console.log('[Room] Chat message:', data);
    io.to(data.room).emit('chat-message', data);
    channel.ack(msg);
  });
}
async function consumeLogs() {
  const channel = getChannel();
  await channel.assertQueue('chat.logger');
  await channel.bindQueue('chat.logger', exchangeName, 'chat.log.*');

  channel.consume('chat.logger', (msg) => {
    const log = JSON.parse(msg.content.toString());
    console.log('[Log] Saved message:', log); // hoặc lưu file, DB, v.v.
    channel.ack(msg);
  });
}
async function consumeHistory() {
  const channel = getChannel();
  await channel.assertQueue('chat.history');
  await channel.bindQueue('chat.history', exchangeName, 'chat.history');

  channel.consume('chat.history', (msg) => {
    const history = JSON.parse(msg.content.toString());
    // gọi DB service để lưu
    console.log('[DB] Save history:', history);
    channel.ack(msg);
  });
}


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
      timestamp: new Date().toISOString()
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
    channel.publish(exchangeName, `chat.history`, Buffer.from(JSON.stringify(message)));

    console.log('Published message to multiple routes via topic exchange');
  };
}


module.exports = { handleChatMessage, consumeHistory, consumeLogs, consumeRoomMessages };
