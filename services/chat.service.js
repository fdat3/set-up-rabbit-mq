const { getChannel, exchangeName } = require('../config/config.rabbitmq');
const { isSpamming } = require('./spam.service');

let queueName = ''; 

async function consumeMessages(io) {
  const channel = getChannel();
  const q = await channel.assertQueue('', { exclusive: true });
  queueName = q.queue;

  channel.consume(queueName, (msg) => {
    if (msg) {
      const data = JSON.parse(msg.content.toString());
      console.log('Received from RabbitMQ:', data);
      io.to(data.room).emit('chat-message', data); 
      channel.ack(msg);
    }
  });
}

function bindRoom(room) {
  const channel = getChannel();
  if (!queueName) {
    console.warn('Queue chưa được khởi tạo!');
    return;
  }
  channel.bindQueue(queueName, exchangeName, room);
  console.log(`[x] Queue bound to room: ${room}`);
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
      timestamp: new Date().toLocaleTimeString()
    };
    console.log('Publishing message to RabbitMQ:', message);
    getChannel().publish(exchangeName, room, Buffer.from(JSON.stringify(message)));
  };
}

module.exports = { handleChatMessage, consumeMessages, bindRoom };
