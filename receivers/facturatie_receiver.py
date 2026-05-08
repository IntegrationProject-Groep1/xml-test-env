"""Facturatie receiver — listens on facturatie.incoming."""
import sys
import threading
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from base_receiver import get_channel, validate_xml, get_msg_type


def listen(timeout: float = 5.0):
    print("[Facturatie] Listening on facturatie.incoming ...")
    connection, channel = get_channel()
    channel.queue_declare(queue="facturatie.incoming", durable=True)
    received = []

    def callback(ch, method, props, body):
        msg_type = get_msg_type(body)
        valid, msg = validate_xml(body, msg_type)
        status = "VALID" if valid else f"INVALID: {msg}"
        print(f"  [Facturatie] {msg_type} — {status}")
        received.append({"type": msg_type, "valid": valid})
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue="facturatie.incoming", on_message_callback=callback, auto_ack=False)
    timer = threading.Timer(timeout, lambda: connection.close())
    timer.start()
    try:
        channel.start_consuming()
    except Exception:
        pass
    finally:
        timer.cancel()
    return received


if __name__ == "__main__":
    listen()
