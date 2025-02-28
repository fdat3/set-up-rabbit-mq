const express = require("express");
const publisher = require("./publisher/publish");

const app = express();
app.use(express.json());
let sequence = 0;

app.post("/process", async (req, res) => {
  try {
    const job = {
      data: `Request at ${new Date().toISOString()}`,
      sequence_number: sequence++,
      timestamp: Date.now()
    };
    // Đẩy job vào job-queue
    const publishResult = await publisher.publishJob(job);
    return res.json({
      error: false,
      data: publishResult
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      message: "Failed to process request"
    });
  }
});

const PORT = 3001;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
