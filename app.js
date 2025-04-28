const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const amqp = require('amqplib');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const RABBITMQ_URL = 'amqp://admin:12345@192.168.1.67:5672';
const exchangeName = 'chat_exchange';
let channel;

// Setup Express static folder
app.use(express.static('public'));

// Kết nối RabbitMQ
async function setupRabbitMQ() {
  const connection = await amqp.connect(RABBITMQ_URL);
  channel = await connection.createChannel();
  await channel.assertExchange(exchangeName, 'direct', { durable: true });
  console.log('Connected to RabbitMQ');
}

// Consume message từ RabbitMQ
async function consumeMessages() {
  const { queue } = await channel.assertQueue('', { exclusive: true }); // random queue
  await channel.bindQueue(queue, exchangeName, '');

  channel.consume(queue, (msg) => {
    if (msg !== null) {
      const data = JSON.parse(msg.content.toString());
      console.log('Received from RabbitMQ:', data);
      io.to(data.room).emit('chat-message', data);
      channel.ack(msg);
    }
  });
}

// Socket.IO
io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  socket.on('join', (room) => {
    socket.join(room);
    console.log(`${socket.id} joined room ${room}`);
  });

  socket.on('chat-message', async ({ room, content }) => {
    const message = {
      room,
      user: socket.id,
      content,
      timestamp: new Date().toLocaleTimeString()
    };
    console.log('Publish to RabbitMQ:', message);

    channel.publish(exchangeName, '', Buffer.from(JSON.stringify(message)));
  });

  socket.on('disconnect', () => {
    console.log('User disconnected:', socket.id);
  });
});

// Start server
server.listen(3000, async () => {
  console.log('Server running at http://localhost:3000');
  await setupRabbitMQ();
  await consumeMessages();
});
