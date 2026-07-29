from fastapi import FastAPI
import aio_pika
import json
import httpx
import asyncio
from dotenv import load_dotenv
import os
import socketio
import logging

load_dotenv()  # Load environment variables from .env file

logger = logging.getLogger("order_processor")
logging.basicConfig(level=logging.INFO)

app = FastAPI()
host = os.getenv("HOST")
port = os.getenv("PORT")
order_api_url = os.environ.get("ORDER_API_URL", "http://localhost:5002")
sio = socketio.AsyncClient()

QUEUE_NAME = "queue1"
DLX_NAME = "dlx.orders"
DEAD_ROUTING_KEY = "orders.process.dead"
RETRY_QUEUE_NAME = "orders.process.retry"
DLQ_NAME = "queue1.dlq"
RETRY_TTL_MS = 30_000  # 30s backoff
MAX_RETRIES = 5


class ProcessingFailed(Exception):
    """Raised so message.process() nacks instead of acking."""
    pass


@app.post('/send_message')
async def send_message(message: str):
    # Connect to your Node.js server
    await sio.connect('http://localhost:5003')
    await sio.emit('message', message)  # Emit the message to the React app
    await sio.disconnect()
    return {"message": "Message sent"}


async def update_order(message_id, updated_message):
    async with httpx.AsyncClient(proxies={}) as client:
        put_url = f"{order_api_url}/{message_id}"
        response = await client.put(put_url, json=updated_message)
        return response


def get_death_count(message: aio_pika.IncomingMessage, queue_name: str) -> int:
    """Read how many times this message has already died on queue_name,
    via the x-death header RabbitMQ stamps on redelivered messages."""
    headers = message.headers or {}
    deaths = headers.get("x-death") or []
    for entry in deaths:
        if entry.get("queue") == queue_name:
            return int(entry.get("count", 0))
    return 0


async def declare_topology(channel: aio_pika.Channel):
    """Declare the DLX + retry queue + terminal DLQ topology.
    Idempotent as long as arguments match on every service that declares it."""
    dlx = await channel.declare_exchange(DLX_NAME, aio_pika.ExchangeType.DIRECT, durable=True)

    main_queue = await channel.declare_queue(
        QUEUE_NAME,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX_NAME,
            "x-dead-letter-routing-key": DEAD_ROUTING_KEY,
        },
    )

    retry_queue = await channel.declare_queue(
        RETRY_QUEUE_NAME,
        durable=True,
        arguments={
            "x-message-ttl": RETRY_TTL_MS,
            "x-dead-letter-exchange": "",  # default exchange
            "x-dead-letter-routing-key": QUEUE_NAME,  # bounces back to queue1 after TTL
        },
    )
    await retry_queue.bind(dlx, routing_key=DEAD_ROUTING_KEY)

    dlq = await channel.declare_queue(DLQ_NAME, durable=True)

    return main_queue, dlx, dlq


async def handle_message(message: aio_pika.IncomingMessage, channel: aio_pika.Channel, dlq):
    death_count = get_death_count(message, QUEUE_NAME)

    if death_count >= MAX_RETRIES:
        # Exhausted retries — don't let it bounce through the DLX/retry
        # loop again. Publish straight to the terminal DLQ and ack it
        # off queue1 so it stops cycling.
        logger.error(
            "Message exceeded max retries (%s), moving to terminal DLQ: %s",
            MAX_RETRIES, message.body[:200],
        )
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=message.body,
                headers=message.headers,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=DLQ_NAME,
        )
        await message.ack()
        return

    async with message.process(requeue=False, ignore_processed=True):
        message_body = message.body.decode()

        try:
            message_object = json.loads(message_body)
        except json.JSONDecodeError as e:
            logger.error("Malformed message body, cannot parse: %s", e)
            raise ProcessingFailed("invalid JSON") from e

        print("Received:", message_object)
        message_id = message_object.get('id', None)
        if message_id is None:
            logger.error("Message missing 'id' field: %s", message_object)
            raise ProcessingFailed("missing id field")

        updated_message = {"is_open": False}

        try:
            response = await update_order(message_id, updated_message)
        except httpx.HTTPError as e:
            logger.warning("Request error updating order %s: %s", message_id, e)
            raise ProcessingFailed("http error") from e

        if response.status_code == 200:
            print(f"Order {message_id} updated successfully.")
        else:
            logger.warning(
                "Failed to update order %s. Status code: %s",
                message_id, response.status_code,
            )
            # This is the key fix: raise instead of just logging, so
            # message.process() nacks (requeue=False) and the message
            # flows into the DLX -> retry queue -> backoff -> queue1 cycle
            # instead of being silently ack'd on a failed update.
            raise ProcessingFailed(f"update_order returned {response.status_code}")


async def consume_messages():
    rabbitmq_url = "amqp://localhost"  # Update with your RabbitMQ server's URL
    connection = await aio_pika.connect_robust(rabbitmq_url)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    queue, dlx, dlq = await declare_topology(channel)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            try:
                await handle_message(message, channel, dlq)
            except ProcessingFailed:
                # Expected control-flow path (message.process() already
                # nacked it above) — nothing else to do here.
                pass
            except Exception:
                logger.exception("Unexpected error handling message")


@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_event_loop()
    loop.create_task(consume_messages())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)
