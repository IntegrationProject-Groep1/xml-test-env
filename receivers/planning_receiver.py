"""Planning receiver — listens on planning.calendar.invite and planning.registration."""
import sys
import threading
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from base_receiver import get_channel, validate_xml, get_msg_type


def listen(timeout: float = 5.0):
    print("[Planning] Listening on planning.calendar.invite + planning.registration ...")
    received = []

    for queue in ("planning.calendar.invite", "planning.registration"):
        connection, channel = get_channel()
        channel.queue_declare(queue=queue, durable=True)

        def make_callback(q):
            def callback(ch, method, props, body):
                msg_type = get_msg_type(body)
                valid, msg = validate_xml(body, msg_type)
                status = "VALID" if valid else f"INVALID: {msg}"
                print(f"  [Planning/{q}] {msg_type} — {status}")
                received.append({"type": msg_type, "valid": valid, "queue": q})
                ch.basic_ack(delivery_tag=method.delivery_tag)
            return callback

        channel.basic_consume(queue=queue, on_message_callback=make_callback(queue), auto_ack=False)
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
