"""Base XML sender with validation and RabbitMQ publishing."""
import uuid
import pika
from datetime import datetime, timezone
from lxml import etree
from pathlib import Path

SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"


def build_message(source: str, msg_type: str, body_xml: str, correlation_id: str = None) -> str:
    message_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    corr = f"<correlation_id>{correlation_id}</correlation_id>" if correlation_id else ""
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<message>\n"
        f"  <header>\n"
        f"    <message_id>{message_id}</message_id>\n"
        f"    <timestamp>{timestamp}</timestamp>\n"
        f"    <source>{source}</source>\n"
        f"    <type>{msg_type}</type>\n"
        f"    <version>2.0</version>\n"
        f"    {corr}\n"
        f"  </header>\n"
        f"  <body>\n"
        f"    {body_xml}\n"
        f"  </body>\n"
        f"</message>"
    )


def validate_xml(xml_str: str, schema_name: str) -> tuple[bool, str]:
    schema_path = SCHEMAS_DIR / f"{schema_name}.xsd"
    if not schema_path.exists():
        return True, f"No schema for {schema_name} — skipping validation"
    try:
        schema_doc = etree.parse(str(schema_path))
        schema = etree.XMLSchema(schema_doc)
        doc = etree.fromstring(xml_str.encode("utf-8"))
        schema.assertValid(doc)
        return True, "OK"
    except etree.DocumentInvalid as e:
        return False, str(e)
    except Exception as e:
        return False, str(e)


def publish(xml_str: str, queue: str = None, exchange: str = "", routing_key: str = "") -> bool:
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials,
            connection_attempts=3, retry_delay=1
        )
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        if queue:
            channel.queue_declare(queue=queue, durable=True)
            channel.basic_publish(
                exchange="", routing_key=queue, body=xml_str,
                properties=pika.BasicProperties(content_type="application/xml", delivery_mode=2)
            )
        else:
            channel.basic_publish(
                exchange=exchange, routing_key=routing_key, body=xml_str,
                properties=pika.BasicProperties(content_type="application/xml", delivery_mode=2)
            )
        connection.close()
        return True
    except Exception as e:
        print(f"  [SEND ERROR] {e}")
        return False
