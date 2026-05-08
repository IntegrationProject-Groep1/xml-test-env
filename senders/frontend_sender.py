"""Frontend XML senders."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from base_sender import build_message, validate_xml, publish


def send_new_registration(identity_uuid: str, email: str, first_name: str, last_name: str,
                          date_of_birth: str, customer_type: str = "private"):
    body = (
        f"<customer>"
        f"<identity_uuid>{identity_uuid}</identity_uuid>"
        f"<email>{email}</email>"
        f"<date_of_birth>{date_of_birth}</date_of_birth>"
        f"<contact><first_name>{first_name}</first_name><last_name>{last_name}</last_name></contact>"
        f"<type>{customer_type}</type>"
        f"</customer>"
        f"<payment_due><amount currency=\"eur\">25.00</amount><status>unpaid</status></payment_due>"
    )
    xml = build_message("frontend", "new_registration", body)
    ok, msg = validate_xml(xml, "new_registration")
    sent = publish(xml, queue="crm.incoming")
    return {"type": "new_registration", "valid": ok, "sent": sent, "msg": msg}


def send_user_created(identity_uuid: str, email: str, first_name: str, last_name: str,
                      date_of_birth: str, customer_type: str = "private"):
    body = (
        f"<customer>"
        f"<identity_uuid>{identity_uuid}</identity_uuid>"
        f"<email>{email}</email>"
        f"<date_of_birth>{date_of_birth}</date_of_birth>"
        f"<contact><first_name>{first_name}</first_name><last_name>{last_name}</last_name></contact>"
        f"<type>{customer_type}</type>"
        f"</customer>"
    )
    xml = build_message("frontend", "user_created", body)
    ok, msg = validate_xml(xml, "user_created")
    sent = publish(xml, queue="crm.incoming")
    return {"type": "user_created", "valid": ok, "sent": sent, "msg": msg}


def send_user_registered(identity_uuid: str, email: str, first_name: str, last_name: str,
                         session_id: str, customer_type: str = "private"):
    body = (
        f"<customer>"
        f"<identity_uuid>{identity_uuid}</identity_uuid>"
        f"<email>{email}</email>"
        f"<contact><first_name>{first_name}</first_name><last_name>{last_name}</last_name></contact>"
        f"<type>{customer_type}</type>"
        f"<session_id>{session_id}</session_id>"
        f"</customer>"
        f"<payment_status>pending</payment_status>"
    )
    xml = build_message("frontend", "user_registered", body)
    ok, msg = validate_xml(xml, "user_registered")
    sent = publish(xml, queue="crm.incoming")
    return {"type": "user_registered", "valid": ok, "sent": sent, "msg": msg}


def send_event_ended(session_id: str, ended_at: str):
    body = f"<session_id>{session_id}</session_id><ended_at>{ended_at}</ended_at>"
    xml = build_message("frontend", "event_ended", body)
    ok, msg = validate_xml(xml, "event_ended")
    sent = publish(xml, queue="crm.incoming")
    return {"type": "event_ended", "valid": ok, "sent": sent, "msg": msg}


def send_calendar_invite(session_id: str, title: str, start_datetime: str, end_datetime: str,
                         location: str = None):
    body = (
        f"<session_id>{session_id}</session_id>"
        f"<title>{title}</title>"
        f"<start_datetime>{start_datetime}</start_datetime>"
        f"<end_datetime>{end_datetime}</end_datetime>"
    )
    if location:
        body += f"<location>{location}</location>"
    xml = build_message("frontend", "calendar_invite", body)
    ok, msg = validate_xml(xml, "calendar_invite")
    sent = publish(xml, exchange="calendar.exchange", routing_key="calendar.invite")
    return {"type": "calendar_invite", "valid": ok, "sent": sent, "msg": msg}
