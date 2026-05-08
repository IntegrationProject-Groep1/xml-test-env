"""Mailing receiver — validates send_mailing messages."""
import sys
import threading
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from base_receiver import get_channel, validate_xml, get_msg_type


def listen(queue: str = "crm.incoming", timeout: float = 5.0):
    print(f"[Mailing] Listening on {queue} ...")
    connection, channel = get_channel()
    channel.queue_declare(queue=queue, durable=True)
    received = []

    def callback(ch, method, props, body):
        msg_type = get_msg_type(body)
        if msg_type != "send_mailing":
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return
        valid, msg = validate_xml(body, "send_mailing")
        status = "VALID" if valid else f"INVALID: {msg}"
        print(f"  [Mailing] {msg_type} — {status}")
        received.append({"type": msg_type, "valid": valid})
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=False)
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
