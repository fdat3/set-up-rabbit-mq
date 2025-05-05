const amqp = require('amqplib');
const RABBITMQ_URL = 'amqp://admin:12345@192.168.1.67:5672';
const exchangeName = 'chat_exchange';

let channel;

async function connectRabbitMQ() {
  const connection = await amqp.connect(RABBITMQ_URL);
  channel = await connection.createChannel();
  await channel.assertExchange(exchangeName, 'direct', { durable: true });
  return channel;
}

module.exports = {
  connectRabbitMQ,
  getChannel: () => channel,
  exchangeName
};
