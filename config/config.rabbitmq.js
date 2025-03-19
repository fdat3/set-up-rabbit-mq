#!/usr/bin/env node

const amqp = require('amqplib/callback_api');

class RabbitMQConfig {
  constructor({
    host = 'localhost',
    port = 5672,
    username = 'guest',
    password = 'guest',
  } = {}) {
    this.url = `amqp://${username}:${password}@${host}:${port}`;
    this.connection = null;
    this.channel = null;
  }

  connect(callback) {
    amqp.connect(this.url, (error, connection) => {
      if (error) {
        console.error('Connection error:', error);
        throw error;
      }
      this.connection = connection;
      this.createChannel(callback);
      console.log('RabbitMQ CONNECT ready!');
    });
  }

  createChannel(callback) {
    this.connection.createChannel((error, channel) => {
      if (error) {
        console.error('Channel creation error:', error);
        throw error;
      }
      this.channel = channel;
      // Không gọi setupQueue() ở đây
      if (callback) callback(this);
    });
  }

  setupQueue(queueName, options = { durable: true }) {
    this.channel.assertQueue(queueName, options);
  }

  sendMessage(queueName, message) {
    if (!this.channel) {
      throw new Error('Channel not initialized. Please connect first.');
    }
    const bufferMessage = Buffer.from(message);
    this.channel.sendToQueue(queueName, bufferMessage, { persistent: true });
    console.log(` [x] Sent ${message} to ${queueName}`);
  }

  close() {
    if (this.connection) {
      this.connection.close();
      console.log('RabbitMQ connection closed');
    }
  }
}

module.exports = RabbitMQConfig;