const messageRateMap = new Map();

function isSpamming(socketId) {
  const now = Date.now();
  const limit = 10; // 10 messages
  const window = 1000; // per 1s

  if (!messageRateMap.has(socketId)) {
    messageRateMap.set(socketId, []);
  }

  const timestamps = messageRateMap
    .get(socketId)
    .filter((ts) => now - ts < window);
  timestamps.push(now);
  messageRateMap.set(socketId, timestamps);

  return timestamps.length > limit;
}

module.exports = { isSpamming };
