const amqp = require("amqplib/callback_api");

// establsh connection with rabbitmq using amqp protocol on default port 5672
amqp.connect("amqp://admin:12345@192.168.1.67:5672", (err0, connection) => {
  if (err0) {
    throw err0;
  }
  // create a channel for data exchange on successfull connection
  connection.createChannel((err1, channel) => {
    if (err1) {
      throw err1;
    }
    // assert the queues to listen to or send data to
    const QUEUE_1 = "PUSH"; // receives data from server
    const QUEUE_2 = "PULL"; // sends data to server
    channel.assertQueue(QUEUE_1);
    channel.assertQueue(QUEUE_2);
    // listen to data coming from server
    channel.consume(
      QUEUE_1,
      (message) => {
        // destructure data
        const data = JSON.parse(message.content.toString());
        // process your data here; business logic
        // ...
        // add response message to PULL queue that sends it to server
        channel.sendToQueue(QUEUE_2, Buffer.from(JSON.stringify(data)));
        // not sending any acknowledgement back to QUEUE_1 (your usecase might require it)
      },
      { noAck: true }
    );
  });
});
