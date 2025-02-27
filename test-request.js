const http = require('http');

const sendRequest = (index) => {
  return new Promise((resolve) => {
    console.log(`Sending request ${index}...`);
    const startTime = Date.now();
    const data = JSON.stringify({ id: `test_value_${index}` });

    const options = {
      hostname: 'localhost',
      port: 3001,
      path: '/process',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
      },
    };

    const req = http.request(options, (res) => {
      console.log(`Request ${index} - Status: ${res.statusCode}`);
      let responseData = '';
      res.on('data', (chunk) => {
        responseData += chunk;
      });
      res.on('end', () => {
        console.log(`Request ${index} completed`);
        const endTime = Date.now();
        resolve({
          index,
          success: res.statusCode === 200,
          data: JSON.parse(responseData),
          time: endTime - startTime,
          timestamp: new Date().toISOString(),
        });
      });
    });

    req.on('error', (error) => {
      console.log(`Request ${index} failed: ${error.message}`);
      resolve({
        index,
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      });
    });

    req.write(data);
    req.end();
  });
};

const testRequests = async () => {
  const totalRequests = 1000;
  const requests = [];

  console.log('Starting test...');
  const startTime = Date.now();

  for (let i = 0; i < totalRequests; i++) {
    requests.push(sendRequest(i));
  }

  const results = await Promise.all(requests);
  const endTime = Date.now();

  const successCount = results.filter(r => r.success).length;
  console.log(`\nTest completed:`);
  console.log(`Total requests: ${totalRequests}`);
  console.log(`Successful requests: ${successCount}`);
  console.log(`Failed requests: ${totalRequests - successCount}`);
  console.log(`Total time: ${endTime - startTime}ms`);

  results
    .filter(r => r.success)
    .sort((a, b) => a.index - b.index)
    .forEach((result) => {
      const jobTimestamp = result.data.data?.originalJob?.timestamp || result.data.data?.timestamp;
      console.log(`Index: ${result.index}, Response: ${JSON.stringify(result.data)}, Job Timestamp: ${jobTimestamp}`);
    });

  console.log('\nChecking FIFO order:', results);
};

testRequests();