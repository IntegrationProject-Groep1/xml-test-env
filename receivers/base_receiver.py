"""Base XML receiver with XSD validation."""
import threading
import pika
from lxml import etree
from pathlib import Path

SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"


def get_channel():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials,
        connection_attempts=3, retry_delay=1
    )
    connection = pika.BlockingConnection(params)
    return connection, connection.channel()


def validate_xml(xml_bytes: bytes, schema_name: str) -> tuple[bool, str]:
    schema_path = SCHEMAS_DIR / f"{schema_name}.xsd"
    if not schema_path.exists():
        return True, f"No schema for {schema_name} — skipping"
    try:
        schema_doc = etree.parse(str(schema_path))
        schema = etree.XMLSchema(schema_doc)
        doc = etree.fromstring(xml_bytes)
        schema.assertValid(doc)
        return True, "valid"
    except etree.DocumentInvalid as e:
        return False, str(e)
    except Exception as e:
        return False, str(e)


def get_msg_type(xml_bytes: bytes) -> str:
    try:
        root = etree.fromstring(xml_bytes)
        el = root.find("header/type")
        return el.text if el is not None else "unknown"
    except Exception:
        return "unknown"


def consume_one(queue: str, timeout: float = 3.0) -> dict | None:
    """Consume a single message from a queue with timeout. Returns None on timeout."""
    result = {"received": False}
    connection, channel = get_channel()
    channel.queue_declare(queue=queue, durable=True)

    def callback(ch, method, properties, body):
        msg_type = get_msg_type(body)
        valid, msg = validate_xml(body, msg_type)
        result["received"] = True
        result["type"] = msg_type
        result["valid"] = valid
        result["validation_msg"] = msg
        ch.basic_ack(delivery_tag=method.delivery_tag)
        connection.close()

    channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=False)

    timer = threading.Timer(timeout, lambda: connection.close())
    timer.start()
    try:
        channel.start_consuming()
    except Exception:
        pass
    finally:
        timer.cancel()

    return result if result["received"] else None
