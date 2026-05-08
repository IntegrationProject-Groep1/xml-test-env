import pika
import socket

def check_rabbit():
    print("--- RabbitMQ Diagnostic Tool ---")
    
    # 1. Check if port 5672 is open and who is listening
    print(f"Checking localhost:5672...")
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect(("localhost", 5672))
        print("  [OK] Port 5672 is open.")
        s.close()
    except Exception as e:
        print(f"  [FAIL] Could not connect to localhost:5672: {e}")
        return

    # 2. Try to connect with Pika
    print("\nConnecting to RabbitMQ...")
    try:
        credentials = pika.PlainCredentials("guest", "guest")
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host="localhost", 
            port=5672,
            credentials=credentials
        ))
        channel = connection.channel()
        print("  [OK] Connected to RabbitMQ.")
        
        # 3. Check for specific queues
        print("\nChecking for crm.incoming queue...")
        try:
            channel.queue_declare(queue="crm.incoming", passive=True)
            print("  [OK] Queue 'crm.incoming' exists.")
        except pika.exceptions.ChannelClosedByBroker as e:
            print(f"  [FAIL] Queue 'crm.incoming' does NOT exist (or something else is wrong: {e})")
            # Reopen channel after error
            connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost", credentials=credentials))
            channel = connection.channel()
        
        # 4. Test a "sticky" message (publish to a queue that should exist)
        print("\nTesting 'sticky' message to 'crm.incoming'...")
        print("Make sure crm-mock is STOPPED for this test.")
        
        test_body = "<test>diagnostic</test>"
        channel.confirm_delivery() # Ensure we get an ACK from broker
        
        try:
            published = channel.basic_publish(
                exchange="",
                routing_key="crm.incoming",
                body=test_body,
                properties=pika.BasicProperties(delivery_mode=2),
                mandatory=True
            )
            if published:
                print("  [OK] Message accepted by broker.")
            else:
                print("  [FAIL] Message was NOT accepted by broker.")
        except pika.exceptions.UnroutableError:
            print("  [FAIL] Message was unroutable (mandatory flag triggered).")
        
        print("\nDiagnostic complete. Now check RabbitMQ UI for 1 message in 'crm.incoming'.")
        connection.close()
        
    except Exception as e:
        print(f"  [ERROR] Diagnostic failed: {e}")

if __name__ == "__main__":
    check_rabbit()
