import os
import pika
import sys
from pathlib import Path
from lxml import etree

SCRIPT_DIR = Path(__file__).parent
EXAMPLES_DIR = SCRIPT_DIR / "examples"
SCHEMAS_DIR = SCRIPT_DIR / "schemas"
RABBITMQ_HOST = "localhost"

# COMPREHENSIVE MAPPING
DESTINATIONS = {
    # (source, type) -> (queue, exchange, routing_key)
    ("frontend", "new_registration"): ("crm.incoming", "", ""),
    ("frontend", "user_created"): ("identity.user.create.request", "", ""),
    ("frontend", "user_registered"): ("crm.incoming", "", ""),
    ("frontend", "user_updated"): ("crm.incoming", "", ""),
    ("frontend", "user_deleted"): ("", "frontend.user.unregistered", ""),
    ("frontend", "calendar_invite"): ("", "calendar.exchange", "frontend.to.planning.calendar.invite"),
    ("frontend", "payment_registered"): ("facturatie.incoming", "", ""),
    ("frontend", "session_view_request"): ("planning.session.events", "planning.exchange", "frontend.to.planning.session.view"),
    ("frontend", "session_create_request"): ("planning.session.events", "planning.exchange", "frontend.to.planning.session.create"),
    ("frontend", "session_update_request"): ("planning.session.events", "planning.exchange", "frontend.to.planning.session.update"),
    ("frontend", "session_delete_request"): ("planning.session.events", "planning.exchange", "frontend.to.planning.session.delete"),
    ("frontend", "wallet_topup_request"): ("crm.incoming", "frontend.exchange", "frontend.to.crm.wallet_topup_request"),
    ("frontend", "cancel_registration"): ("planning.calendar.invite", "calendar.exchange", "frontend.to.planning.cancel_registration"),
    ("frontend", "event_ended"): ("facturatie.incoming", "", ""),
    ("kassa", "payment_registered"): ("crm.incoming", "kassa.exchange", "kassa.payments.registration"),
    ("kassa", "consumption_order"): ("crm.incoming", "kassa.exchange", "kassa.payments.consumption"),
    ("kassa", "refund_processed"): ("crm.incoming", "kassa.exchange", "kassa.payments.refund"),
    ("kassa", "badge_assigned"): ("crm.incoming", "kassa.exchange", "kassa.payments.badge"),
    ("kassa", "wallet_lease_request"): ("crm.incoming", "kassa.exchange", "kassa.to.crm.wallet_lease_request"),
    ("kassa", "wallet_lease_return"): ("crm.incoming", "kassa.exchange", "kassa.to.crm.wallet_lease_return"),
    ("kassa", "payment_status"): ("frontend.incoming", "", ""),
    ("kassa", "invoice_request"): ("facturatie.incoming", "", ""),
    ("iot_gateway", "badge_scanned"): ("kassa.incoming", "kassa.exchange", "kassa.incoming"),
    ("planning", "session_occupancy_update"): ("planning.session.events", "planning.exchange", "planning.session.occupancy"),
    ("planning", "session_updated"): ("planning.session.events", "planning.exchange", "planning.session.updated"),
    ("planning", "session_created"): ("planning.session.events", "planning.exchange", "planning.session.created"),
    ("planning", "session_deleted"): ("planning.session.events", "planning.exchange", "planning.session.deleted"),
    ("planning", "calendar_invite_confirmed"): ("", "calendar.exchange", "planning.to.frontend.calendar.invite.confirmed"),
    ("planning", "system_error"): ("logs", "", ""),
    ("crm", "new_registration"): ("kassa.incoming", "kassa.exchange", ""),
    ("crm", "invoice_request"): ("facturatie.incoming", "", ""),
    ("crm", "invoice_cancelled"): ("facturatie.incoming", "", ""),
    ("crm", "send_mailing"): ("crm.to.mailing", "", ""),
    ("crm", "cancel_registration"): ("planning.calendar.invite", "calendar.exchange", "crm.to.planning.cancel_registration"),
    ("crm", "wallet_balance_update"): ("", "wallet.updates", ""),
    ("crm", "wallet_lease_grant"): ("kassa.incoming", "crm.exchange", "crm.to.kassa.wallet_lease_grant"),
    ("crm", "wallet_remote_topup"): ("kassa.incoming", "crm.exchange", "crm.to.kassa.wallet_remote_topup"),
    ("facturatie", "invoice_status"): ("facturatie.to.crm", "", ""),
    ("facturatie", "invoice_available"): ("facturatie.to.frontend", "", ""),
    ("facturatie", "send_mailing"): ("facturatie.to.mailing", "", ""),
    ("facturatie", "payment_registered"): ("crm.incoming", "", ""),
    ("identity", "user_created"): ("", "user.events", ""),
    ("monitoring", "alert"): ("to_mailing", "", ""),
    ("monitoring", "vat_validation_error"): ("to_mailing", "", ""),
    ("mailing", "mailing_status"): ("facturatie.to.crm", "", ""),
    ("*", "log"): ("logs", "", ""),
    ("*", "heartbeat"): ("heartbeat", "", ""),
    ("*", "system_error"): ("logs", "", ""),
}

def get_msg_info(xml_content):
    try:
        root = etree.fromstring(xml_content)
        source = root.findtext("header/source")
        msg_type = root.findtext("header/type")
        return source, msg_type
    except Exception:
        return None, None

def run_tests(dry_run=False):
    examples = list(EXAMPLES_DIR.glob("*.xml"))
    print(f"Running contract tests ({'DRY RUN' if dry_run else 'LIVE'})...\n")
    
    results = []
    
    connection = None
    channel = None
    if not dry_run:
        try:
            credentials = pika.PlainCredentials("guest", "guest")
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=RABBITMQ_HOST, 
                port=5672,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            ))
            channel = connection.channel()
            print("Successfully connected to RabbitMQ broker.")
        except Exception as e:
            print(f"Could not connect to RabbitMQ: {e}")
            print("Switching to dry-run mode.\n")
            dry_run = True

    for example_path in examples:
        base_name = example_path.stem
        with open(example_path, "rb") as f:
            xml_content = f.read()
            
        source, msg_type = get_msg_info(xml_content)
        if not source or not msg_type:
            continue
            
        # Validation
        schema_path = SCHEMAS_DIR / f"{base_name}.xsd"
        if not schema_path.exists():
            generic_name = base_name.rsplit('_', 1)[0]
            schema_path = SCHEMAS_DIR / f"{generic_name}.xsd"
            
        valid = False
        if schema_path.exists():
            try:
                schema_doc = etree.parse(str(schema_path))
                schema = etree.XMLSchema(schema_doc)
                doc = etree.fromstring(xml_content)
                schema.assertValid(doc)
                valid = True
            except Exception:
                valid = False
        
        # Determine destination
        dest = DESTINATIONS.get((source, msg_type))
        if not dest:
            dest = DESTINATIONS.get(("*", msg_type))
            
        dest_str = "Unknown destination"
        if dest:
            q, ex, rk = dest
            dest_str = f"Q:{q} EX:{ex} RK:{rk}"
        
        status = "VALID" if valid else "INVALID"
        
        if not dry_run and valid and dest:
            q, ex, rk = dest
            try:
                channel.basic_publish(
                    exchange=ex, 
                    routing_key=rk or q, 
                    body=xml_content,
                    properties=pika.BasicProperties(
                        content_type="application/xml", 
                        delivery_mode=2
                    ),
                    mandatory=True
                )
                print(f"  [{status}] {base_name} -> {dest_str} -> SENT")
            except Exception as e:
                print(f"  [{status}] {base_name} -> {dest_str} -> SEND ERROR: {e}")
        else:
            print(f"  [{status}] {base_name} -> {dest_str}")
                
        results.append((base_name, valid, bool(dest)))

    if connection:
        connection.close()

    total = len(results)
    valid_count = sum(1 for r in results if r[1])
    mapped_count = sum(1 for r in results if r[2])
    print(f"\nSummary: {valid_count}/{total} valid examples, {mapped_count}/{total} mapped to destinations.")

if __name__ == "__main__":
    is_dry = "--live" not in sys.argv
    run_tests(dry_run=is_dry)
