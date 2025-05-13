const io = require('socket.io-client');
const async = require('async');
const os = require('os');

// Cấu hình
const SERVER_URL = 'http://localhost:3000';
const NUM_CLIENTS = 100; // Số lượng client giả lập
const MESSAGES_PER_CLIENT = 10; // Số message mỗi client gửi
const TOTAL_MESSAGES = NUM_CLIENTS * MESSAGES_PER_CLIENT; // Tổng số message: 1000
const ROOM = 'test-room';

// Lưu thông số
let startTime;
let receivedMessages = 0;
let totalBatches = 0;
let batchTimes = []; // Lưu thời gian xử lý từng batch
let initialCPU = os.loadavg()[0];
let initialMem = (os.totalmem() - os.freemem()) / os.totalmem();

// Hàm tạo client và gửi message
function createClient(clientId, callback) {
  const socket = io(SERVER_URL);

  socket.on('connect', () => {
    console.log(`Client ${clientId} connected`);
    socket.emit('join', ROOM);

    // Gửi message
    let messagesSent = 0;
    const sendMessage = () => {
      if (messagesSent < MESSAGES_PER_CLIENT) {
        socket.emit('chat-message', {
          room: ROOM,
          content: `Message ${messagesSent + 1} from client ${clientId}`,
        });
        messagesSent++;
        setTimeout(sendMessage, 10); // Gửi message tiếp theo sau 10ms
      }
    };

    sendMessage();
  });

  socket.on('chat-message', (data) => {
    console.log(`Client ${clientId} received: ${data.content}`);
    receivedMessages++;

    // Khi nhận đủ message, ngắt kết nối
    if (receivedMessages === TOTAL_MESSAGES) {
      const endTime = Date.now();
      const finalCPU = os.loadavg()[0];
      const finalMem = (os.totalmem() - os.freemem()) / os.totalmem();

      console.log(`\n[Performance Test Results]`);
      console.log(`Total messages sent: ${TOTAL_MESSAGES}`);
      console.log(`Total messages received: ${receivedMessages}`);
      console.log(`Total batches processed: ${totalBatches}`);
      console.log(`Average batch size: ${TOTAL_MESSAGES / totalBatches || 0}`);
      console.log(`Total time: ${(endTime - startTime) / 1000} seconds`);
      console.log(`Average latency per message: ${(endTime - startTime) / TOTAL_MESSAGES} ms`);
      console.log(`Average batch processing time: ${batchTimes.reduce((a, b) => a + b, 0) / batchTimes.length || 0} ms`);
      console.log(`Initial CPU Load: ${initialCPU.toFixed(2)}`);
      console.log(`Final CPU Load: ${finalCPU.toFixed(2)}`);
      console.log(`CPU Load Increase: ${(finalCPU - initialCPU).toFixed(2)}`);
      console.log(`Initial RAM Usage: ${(initialMem * 100).toFixed(2)}%`);
      console.log(`Final RAM Usage: ${(finalMem * 100).toFixed(2)}%`);
      console.log(`RAM Usage Increase: ${((finalMem - initialMem) * 100).toFixed(2)}%`);

      socket.disconnect();
      callback();
    }
  });

  socket.on('spam-warning', (msg) => {
    console.log(`Client ${clientId} received spam warning: ${msg}`);
  });

  socket.on('disconnect', () => {
    console.log(`Client ${clientId} disconnected`);
  });
}

// Hàm chạy test
function runTest() {
  console.log(`Starting test with ${NUM_CLIENTS} clients, ${TOTAL_MESSAGES} messages...`);
  startTime = Date.now();

  // Tạo nhiều client song song
  const clients = Array.from({ length: NUM_CLIENTS }, (_, i) => (callback) =>
    createClient(i + 1, callback)
  );

  async.parallel(clients, (err) => {
    if (err) {
      console.error('Error during test:', err);
    } else {
      console.log('Test completed');
    }
    process.exit(0);
  });
}

// Export để kết nối với chat.service.js (nếu cần)
require('./services/'); // Đảm bảo consumeHistory đang chạy và cập nhật totalBatches, batchTimes

// Chạy test
runTest();