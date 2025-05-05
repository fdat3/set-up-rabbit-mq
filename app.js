const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const os = require('os');
const { connectRabbitMQ } = require('./config/config.rabbitmq');
const { handleChatMessage, consumeMessages, bindRoom } = require('./services/chat.service');
const { clearSocketId } = require('./services/spam.service');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  socket.on('join', (room) => {
    socket.join(room);
    console.log(`${socket.id} joined room ${room}`);
  });

  socket.on('join', (room) => {
    socket.join(room);
    bindRoom(room);
    console.log(`${socket.id} joined room ${room}`);
  });

  socket.on('chat-message', (data) => {
    handleChatMessage(io, socket)(data); 
  });

  socket.on('disconnect', () => {
    console.log('User disconnected:', socket.id);
    clearSocketId(socket.id); // cleanup
  });
});

setInterval(() => {
  const memUsage = (os.totalmem() - os.freemem()) / os.totalmem();
  console.log(`[System] CPU Load: ${os.loadavg()[0].toFixed(2)}, RAM Usage: ${(memUsage * 100).toFixed(2)}%`);
}, 10000);

server.listen(3000, async () => {
  console.log('Server running at http://localhost:3000');
  await connectRabbitMQ();
  await consumeMessages(io);
});
