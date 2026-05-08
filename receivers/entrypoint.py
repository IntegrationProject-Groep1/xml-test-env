import os
import time
import pika
import sys
from lxml import etree
from pathlib import Path

# Config from environment
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = os.getenv("QUEUE_NAME", "test_queue")
SERVICE_NAME = os.getenv("SERVICE_NAME", "generic_service")

SCHEMAS_DIR = Path("/app/schemas")

def get_msg_type(xml_bytes):
    try:
        root = etree.fromstring(xml_bytes)
        el = root.find("header/type")
        return el.text if el is not None else "unknown"
    except Exception:
        return "unknown"

def validate_xml(xml_bytes, schema_name):
    schema_path = SCHEMAS_DIR / f"{schema_name}.xsd"
    
    if not schema_path.exists():
        potential = list(SCHEMAS_DIR.glob(f"{schema_name}_*.xsd"))
        if potential:
            schema_path = potential[0]
            
    if not schema_path.exists():
        return False, f"Schema {schema_name}.xsd not found"

    try:
        schema_doc = etree.parse(str(schema_path))
        schema = etree.XMLSchema(schema_doc)
        doc = etree.fromstring(xml_bytes)
        schema.assertValid(doc)
        return True, "Valid"
    except etree.DocumentInvalid as e:
        return False, str(e)
    except Exception as e:
        return False, str(e)

def callback(ch, method, properties, body):
    msg_type = get_msg_type(body)
    # Gebruik flush=True om er zeker van te zijn dat de logs direct verschijnen
    print(f"\n[{SERVICE_NAME}] >>> ONTVANGEN: {msg_type}", flush=True)
    
    valid, result = validate_xml(body, msg_type)
    
    if valid:
        print(f"[{SERVICE_NAME}]   [✅ VALID] Bericht voldoet aan contract.", flush=True)
    else:
        print(f"[{SERVICE_NAME}]   [❌ INVALID] Contractbreuk: {result}", flush=True)
        
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    print(f"[{SERVICE_NAME}] Starten... luisteren op queue: {QUEUE_NAME}", flush=True)
    
    connected = False
    while not connected:
        try:
            credentials = pika.PlainCredentials("guest", "guest")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            connected = True
        except Exception as e:
            print(f"[{SERVICE_NAME}] Wachten op RabbitMQ ({e})...", flush=True)
            time.sleep(2)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    print(f"[{SERVICE_NAME}] Gereed voor XML berichten.", flush=True)
    channel.start_consuming()

if __name__ == "__main__":
    main()
