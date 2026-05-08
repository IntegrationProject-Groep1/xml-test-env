"""Planning XML senders."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from base_sender import build_message, validate_xml, publish


def send_session_created(session_id: str, title: str, start_datetime: str, end_datetime: str,
                          location: str, max_attendees: int = 120):
    body = (
        f"<session_id>{session_id}</session_id>"
        f"<title>{title}</title>"
        f"<start_datetime>{start_datetime}</start_datetime>"
        f"<end_datetime>{end_datetime}</end_datetime>"
        f"<location>{location}</location>"
        f"<session_type>keynote</session_type>"
        f"<status>published</status>"
        f"<max_attendees>{max_attendees}</max_attendees>"
        f"<current_attendees>0</current_attendees>"
    )
    xml = build_message("planning", "session_created", body)
    ok, msg = validate_xml(xml, "session_created")
    sent = publish(xml, exchange="planning.exchange", routing_key="planning.session.created")
    return {"type": "session_created", "valid": ok, "sent": sent, "msg": msg}


def send_session_occupancy_update(session_id: str, current_attendees: int, max_attendees: int,
                                   status: str = "available"):
    body = (
        f"<session_id>{session_id}</session_id>"
        f"<current_attendees>{current_attendees}</current_attendees>"
        f"<max_attendees>{max_attendees}</max_attendees>"
        f"<status>{status}</status>"
    )
    xml = build_message("planning", "session_occupancy_update", body)
    ok, msg = validate_xml(xml, "session_occupancy_update")
    sent = publish(xml, exchange="planning.exchange", routing_key="planning.session.occupancy")
    return {"type": "session_occupancy_update", "valid": ok, "sent": sent, "msg": msg}
