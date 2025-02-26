require('dotenv').config();
const { Connection } = require('rabbitmq-client');

function createConnection(name) {
  const rabbit = new Connection(process.env.RABBIT_MQ_URL);

  rabbit.on('connection', () => {
    console.log('Connection successfully (re)established')
  })

  rabbit.on('error', (err) => {
    console.error(`RabbitMQ ${name} connection error:`, err);
  });

  rabbit.on('close', () => {
    console.log(`RabbitMQ ${name} connection closed`);
  });

  return rabbit;
}

module.exports = { createConnection };