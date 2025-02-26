const express = require('express');
const publisher = require('./publisher/publish');

const app = express();
app.use(express.json());

app.post('/process', async (req, res) => {
  try {
    const job = {
      id: Date.now(),
      data: req.body,
      timestamp: new Date().toISOString()
    };

    // Đẩy job vào job-queue
    const publishResult = await publisher.publishJob(job);
    if (publishResult.error) {
      return res.status(500).json({
        error: true,
        data: null,
        msg: 'Failed to queue job'
      });
    }

    const result = await publisher.getResultFromQueue();
    return res.json({
      error: result.error,
      data: result.data,
      msg: result.msg
    });

  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Failed to process request'
    });
  }
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

process.on('SIGINT', async () => {
  await publisher.close();
  process.exit(0);
});