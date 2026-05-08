"""Declare all RabbitMQ queues and exchanges used by the integration project."""
import pika

RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"

QUEUES = [
    "crm.incoming",
    "kassa.incoming",
    "facturatie.incoming",
    "planning.calendar.invite",
    "planning.registration",
    "logs",
    "kassa.errors",
    "frontend.incoming",
    "frontend.payments",
]

TOPIC_EXCHANGES = [
    "planning.exchange",
    "calendar.exchange",
    "crm.exchange",
    "kassa.exchange",
]

FANOUT_EXCHANGES = [
    "wallet.updates",
    "user.events",
]


def main():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials,
        connection_attempts=5, retry_delay=2
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    for queue in QUEUES:
        channel.queue_declare(queue=queue, durable=True)
        print(f"  Queue declared: {queue}")

    for exchange in TOPIC_EXCHANGES:
        channel.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
        print(f"  Exchange declared (topic): {exchange}")

    for exchange in FANOUT_EXCHANGES:
        channel.exchange_declare(exchange=exchange, exchange_type="fanout", durable=True)
        print(f"  Exchange declared (fanout): {exchange}")

    channel.queue_bind(queue="planning.calendar.invite", exchange="calendar.exchange", routing_key="calendar.invite")
    channel.queue_bind(queue="crm.incoming", exchange="planning.exchange", routing_key="planning.session.created")
    channel.queue_bind(queue="crm.incoming", exchange="planning.exchange", routing_key="planning.session.occupancy")
    channel.queue_bind(queue="frontend.payments", exchange="wallet.updates", routing_key="")

    connection.close()
    print("\nAll queues and exchanges declared.")


if __name__ == "__main__":
    main()
