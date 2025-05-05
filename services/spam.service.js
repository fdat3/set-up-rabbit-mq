const messageRateMap = new Map();

function isSpamming(socketId) {
  const now = Date.now();
  const limit = 10; // tối đa 10 tin nhắn
  const window = 1000; // trong 1 giây

  if (!messageRateMap.has(socketId)) {
    messageRateMap.set(socketId, []);
  }

  const timestamps = messageRateMap.get(socketId).filter((ts) => now - ts < window);
  timestamps.push(now);
  messageRateMap.set(socketId, timestamps);

  return timestamps.length > limit;
}

function clearSocketId(socketId) {
  messageRateMap.delete(socketId);
}

// Cleanup định kỳ socket không còn hoạt động
setInterval(() => {
  const now = Date.now();
  for (const [socketId, timestamps] of messageRateMap.entries()) {
    const recent = timestamps.filter((ts) => now - ts < 1000);
    if (recent.length === 0) {
      messageRateMap.delete(socketId);
    } else {
      messageRateMap.set(socketId, recent);
    }
  }
}, 60000); // mỗi 60 giây

module.exports = {
  isSpamming,
  clearSocketId
};
