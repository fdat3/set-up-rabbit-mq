const amqp = require('amqplib');
const RABBITMQ_URL = 'amqps://makvpqxt:nNPeTCRo_LVboxIkNXff1vDzjui87Koz@fuji.lmq.cloudamqp.com/makvpqxt';
const exchangeName = 'chat_exchange';

let channel;

async function connectRabbitMQ() {
  const connection = await amqp.connect(RABBITMQ_URL);
  channel = await connection.createChannel();
  await channel.assertExchange(exchangeName, 'topic', { durable: true });
  return channel;
}

module.exports = {
  connectRabbitMQ,
  getChannel: () => channel,
  exchangeName
};
