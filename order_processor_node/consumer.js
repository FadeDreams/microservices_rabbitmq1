const amqp = require('amqplib');
const axios = require('axios');
const dotenv = require('dotenv');
dotenv.config();

const order_api_url = process.env.ORDER_API_URL || 'http://localhost:5002';
const queueName = 'queue1';
const exchangeName = 'queue1.exchange';
const dlxName = 'queue1.dlx';
const retryQueueName = 'queue1.retry';
const deadQueueName = 'queue1.dead';
const rabbitMQHost = process.env.RABBITMQ_HOST || 'amqp://localhost';

const MAX_RETRIES = 5;
const RETRY_TTL_MS = 30000; // 30s backoff between retries

function getDeathCount(msg) {
  const xDeath = msg.properties?.headers?.['x-death'];
  if (!xDeath) return 0;
  const entry = xDeath.find((d) => d.queue === queueName);
  return entry?.count ?? 0;
}

async function consumeMessages() {
  console.log('consumeMessages');
  const connection = await amqp.connect(rabbitMQHost);
  const channel = await connection.createChannel();

  await channel.assertExchange(exchangeName, 'direct', { durable: true });
  await channel.assertExchange(dlxName, 'direct', { durable: true });

  // Main queue, dead-letters into the retry flow on nack
  await channel.assertQueue(queueName, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': dlxName,
      'x-dead-letter-routing-key': 'queue1.retry',
    },
  });
  await channel.bindQueue(queueName, exchangeName, queueName);

  // Retry queue: holds message for RETRY_TTL_MS, then dead-letters back to the main queue
  await channel.assertQueue(retryQueueName, {
    durable: true,
    arguments: {
      'x-message-ttl': RETRY_TTL_MS,
      'x-dead-letter-exchange': exchangeName,
      'x-dead-letter-routing-key': queueName,
    },
  });
  await channel.bindQueue(retryQueueName, dlxName, 'queue1.retry');

  // Terminal dead queue: no further DLX, poison messages land here for good
  await channel.assertQueue(deadQueueName, { durable: true });
  await channel.bindQueue(deadQueueName, dlxName, 'queue1.dead');

  console.log(`[*] Waiting for messages in ${queueName}. To exit, press Ctrl-C`);

  channel.consume(queueName, async (msg) => {
    if (msg === null) return;

    let messageObject;
    try {
      const message = msg.content.toString();
      messageObject = JSON.parse(message);
      console.log('[x] Received', messageObject);
    } catch (parseError) {
      // Malformed JSON will never parse successfully no matter how many
      // times we retry, so send straight to the dead queue.
      console.error('Error parsing message, routing to dead queue:', parseError);
      channel.publish(dlxName, 'queue1.dead', msg.content, { headers: msg.properties.headers });
      channel.ack(msg);
      return;
    }

    try {
      messageObject.is_open = false;
      const putUrl = `${order_api_url}/${messageObject.id}`;
      console.log(putUrl, messageObject);
      const response = await axios.put(putUrl, messageObject);
      console.log('Response:', response.data);
      channel.ack(msg); // only ack after the PUT actually succeeds
    } catch (error) {
      const deathCount = getDeathCount(msg);
      console.error(`Error sending PUT request (attempt ${deathCount + 1}):`, error.message);

      if (deathCount >= MAX_RETRIES) {
        channel.publish(dlxName, 'queue1.dead', msg.content, { headers: msg.properties.headers });
        channel.ack(msg);
      } else {
        channel.nack(msg, false, false); // -> DLX -> retry queue -> back to main queue after TTL
      }
    }
  });
}

module.exports = consumeMessages;
