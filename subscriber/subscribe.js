const { createConnection } = require('../config/config.rabbitmq');
const jobService = require("../services/job.service");

class Consumer {
  constructor() {
    this.rabbit = createConnection('consumer');
    this.setup();
  }

  async setup() {
    this.rabbit.on('error', (err) => {
      console.error('RabbitMQ connection error:', err);
    });

    this.rabbit.on('connection', () => {
      console.log('Consumer connected to RabbitMQ');
      this.rabbit.queueDeclare({ queue: 'job-queue', durable: true });
      this.rabbit.queueDeclare({ queue: 'result-queue', durable: true });
    });

    await this.rabbit.createConsumer({
      queue: 'job-queue',
      queueOptions: { durable: true },
      qos: { prefetchCount: 1 },
    }, async (msg) => {
      try {
        const job = msg.body;
        const result = await jobService.jobQueueMQ(job);

        const pub = await this.rabbit.createPublisher('result-queue', {
          routingKey: 'result',
          body: Buffer.from(JSON.stringify({
            jobId: job.id,
            error: result.error,
            data: result.data,
            code: result.code,
            msg: result.msg
          }))
        });
        await pub.send('result-queue', {
          jobId: job.id,
          error: result.error,
          data: result.data,
          code: result.code,
          msg: result.msg
        });

        return { error: false, data: result };
      } catch (error) {
        console.error('Error processing message:', error);
      }
    });
  }
}

const consumer = new Consumer();

process.on('SIGINT', async () => {
  await consumer.close();
  process.exit(0);
});