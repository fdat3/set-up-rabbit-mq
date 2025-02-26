const { createConnection } = require('../config/config.rabbitmq');

class Publisher {
  constructor() {
    this.rabbit = createConnection('publisher');
    this.setupEventListeners();
  }

  setupEventListeners() {
    this.rabbit.on('error', (err) => {
      console.error('RabbitMQ connection error:', err);
    });

    this.rabbit.on('connection', () => {
      console.log('Publisher connected to RabbitMQ');
      this.rabbit.queueDeclare({ queue: 'job-queue', durable: true });
      this.rabbit.queueDeclare({ queue: 'result-queue', durable: true });
    });
  }

  async publishJob(job) {
    try {
      const pub = await this.rabbit.createPublisher('job-queue', {
        routingKey: 'process',
        body: Buffer.from(JSON.stringify(job)),
        properties: {
          deliveryMode: 2,
          contentType: 'application/json'
        }
      });
      await pub.send('job-queue', { id: job.id });
      return { error: false, data: { jobId: job.id } };
    } catch (error) {
      console.error('Error publishing to queue:', error);
      return { error: true, data: null, msg: 'Failed to publish job' };
    }
  }

  async getResultFromQueue() {
    return new Promise((resolve) => {
      this.rabbit.createConsumer({
        queue: 'result-queue',
        queueOptions: { durable: true },
        qos: { prefetchCount: 1 },
      }, async (msg) => {
        try {
          const result = msg.body;
          resolve(result);
        } catch (error) {
          console.error('Error processing result:', error);
          resolve({ error: true, data: null, msg: 'Failed to process result' });
        }
      });
    });
  }

  async close() {
    await this.rabbit.close();
  }
}

module.exports = new Publisher();