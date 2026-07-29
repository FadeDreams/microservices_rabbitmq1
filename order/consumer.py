import pika
import sys
import os
import json

QUEUE_NAME = "queue1"
EXCHANGE_NAME = "queue1.exchange"
DLX_NAME = "queue1.dlx"
RETRY_QUEUE_NAME = "queue1.retry"
DEAD_QUEUE_NAME = "queue1.dead"

MAX_RETRIES = 5
RETRY_TTL_MS = 30000  # 30s backoff between retries


def get_death_count(properties, queue_name):
    headers = properties.headers or {}
    x_death = headers.get("x-death")
    if not x_death:
        return 0
    for entry in x_death:
        if entry.get("queue") == queue_name:
            return entry.get("count", 0)
    return 0


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get("RABBITMQ_HOST", "localhost"))
    )
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=DLX_NAME, exchange_type="direct", durable=True)

    # Main queue, dead-letters into the retry flow on nack
    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX_NAME,
            "x-dead-letter-routing-key": "queue1.retry",
        },
    )
    channel.queue_bind(queue=QUEUE_NAME, exchange=EXCHANGE_NAME, routing_key=QUEUE_NAME)

    # Retry queue: holds message for RETRY_TTL_MS, then dead-letters back to the main queue
    channel.queue_declare(
        queue=RETRY_QUEUE_NAME,
        durable=True,
        arguments={
            "x-message-ttl": RETRY_TTL_MS,
            "x-dead-letter-exchange": EXCHANGE_NAME,
            "x-dead-letter-routing-key": QUEUE_NAME,
        },
    )
    channel.queue_bind(queue=RETRY_QUEUE_NAME, exchange=DLX_NAME, routing_key="queue1.retry")

    # Terminal dead queue: no further DLX, poison messages land here for good
    channel.queue_declare(queue=DEAD_QUEUE_NAME, durable=True)
    channel.queue_bind(queue=DEAD_QUEUE_NAME, exchange=DLX_NAME, routing_key="queue1.dead")

    def callback(ch, method, properties, body):
        try:
            message = body.decode("utf-8")
            message_dict = json.loads(message)
            print("[x] received %r" % message_dict)

            # TODO: do the actual processing here (e.g. call the order API).
            # If that call can fail, wrap it in its own try/except below so
            # parse errors and processing errors are told apart.

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError as parse_error:
            # Malformed JSON will never parse successfully no matter how many
            # times we retry, so send straight to the dead queue.
            print(f"Error parsing message, routing to dead queue: {parse_error}")
            ch.basic_publish(
                exchange=DLX_NAME,
                routing_key="queue1.dead",
                body=body,
                properties=properties,
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as error:
            death_count = get_death_count(properties, QUEUE_NAME)
            print(f"Error processing message (attempt {death_count + 1}): {error}")

            if death_count >= MAX_RETRIES:
                ch.basic_publish(
                    exchange=DLX_NAME,
                    routing_key="queue1.dead",
                    body=body,
                    properties=properties,
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                # requeue=False -> DLX -> retry queue -> back to main queue after TTL
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)

    print(" [*] waiting for the messages. To exit press Ctrl-C")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
