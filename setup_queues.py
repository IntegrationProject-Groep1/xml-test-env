"""Declare all RabbitMQ queues and exchanges based on the v2.3 Contract + Code Analysis."""
import pika

RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"

# Comprehensive list of Queues discovered in code/contract
QUEUES = [
    "crm.incoming",
    "kassa.incoming",
    "facturatie.incoming",
    "mailing.incoming",
    "planning.calendar.invite",
    "planning.registration",
    "planning.session.events",
    "frontend.incoming",
    "frontend.payments",
    "frontend.user_created",
    "identity.user.create.request",
    "identity.user.lookup.email.request",
    "identity.user.lookup.uuid.request",
    "heartbeat",
    "logs",
    "to_mailing",
    "kassa.errors",
    "mailing.errors",
    "crm.dead-letter",
    "facturatie.dlq",
    "planning.dlx",
    "crm.salesforce",
    "planning.outlook",
    "mailing.sendgrid",
]

# Exchanges discovered
EXCHANGES = [
    ("kassa.exchange", "topic"),
    ("planning.exchange", "topic"),
    ("calendar.exchange", "topic"),
    ("crm.exchange", "topic"),
    ("frontend.exchange", "topic"),
    ("wallet.updates", "fanout"),
    ("user.events", "fanout"),
    ("frontend.user.unregistered", "fanout"),
]

def main():
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials,
            connection_attempts=5, retry_delay=2
        )
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # Declare Queues
        for queue in QUEUES:
            channel.queue_declare(queue=queue, durable=True)
            print(f"  Queue declared: {queue}")

        # Declare Exchanges
        for exchange_name, exchange_type in EXCHANGES:
            channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=True)
            print(f"  Exchange declared ({exchange_type}): {exchange_name}")

        # --- Bindings ---
        
        # Kassa bindings
        channel.queue_bind(queue="crm.incoming", exchange="kassa.exchange", routing_key="kassa.#")
        channel.queue_bind(queue="kassa.incoming", exchange="kassa.exchange", routing_key="kassa.incoming")
        channel.queue_bind(queue="frontend.incoming", exchange="kassa.exchange", routing_key="kassa.frontend.#")
        
        # Planning bindings
        # Mock Planning listens on planning.session.events
        channel.queue_bind(queue="planning.session.events", exchange="planning.exchange", routing_key="#")
        channel.queue_bind(queue="planning.session.events", exchange="calendar.exchange", routing_key="#")
        
        # CRM bindings
        channel.queue_bind(queue="crm.incoming", exchange="frontend.exchange", routing_key="frontend.to.crm.#")
        channel.queue_bind(queue="crm.incoming", exchange="kassa.exchange", routing_key="kassa.to.crm.#")
        channel.queue_bind(queue="kassa.incoming", exchange="crm.exchange", routing_key="crm.to.kassa.#")
        
        # Identity bindings (fanout)
        channel.queue_bind(queue="frontend.incoming", exchange="user.events", routing_key="")
        channel.queue_bind(queue="crm.incoming", exchange="user.events", routing_key="")
        
        # CRM Fanout (unregistered)
        channel.queue_bind(queue="crm.incoming", exchange="frontend.user.unregistered", routing_key="")

        # Wallet updates (fanout)
        channel.queue_bind(queue="frontend.incoming", exchange="wallet.updates", routing_key="")

        connection.close()
        print("\nAll queues, exchanges and bindings declared (Comprehensive Sync).")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
