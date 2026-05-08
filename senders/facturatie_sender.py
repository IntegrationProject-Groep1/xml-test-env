"""Facturatie XML senders."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from base_sender import build_message, validate_xml, publish


def send_invoice_available(identity_uuid: str, invoice_id: str, pdf_url: str,
                            correlation_id: str = None):
    body = (
        f"<identity_uuid>{identity_uuid}</identity_uuid>"
        f"<invoice_id>{invoice_id}</invoice_id>"
        f"<pdf_url>{pdf_url}</pdf_url>"
    )
    xml = build_message("facturatie", "invoice_available", body, correlation_id=correlation_id)
    ok, msg = validate_xml(xml, "invoice_available")
    sent = publish(xml, queue="frontend.incoming")
    return {"type": "invoice_available", "valid": ok, "sent": sent, "msg": msg}


def send_send_mailing(campaign_id: str, subject: str, mail_type: str, recipients: list,
                      correlation_id: str = None):
    recipients_xml = "".join(
        f"<recipient>"
        f"<email>{r['email']}</email>"
        f"<user_id>{r['user_id']}</user_id>"
        f"<contact><first_name>{r['first_name']}</first_name>"
        f"<last_name>{r['last_name']}</last_name></contact>"
        f"</recipient>"
        for r in recipients
    )
    body = (
        f"<campaign_id>{campaign_id}</campaign_id>"
        f"<subject>{subject}</subject>"
        f"<mail_type>{mail_type}</mail_type>"
        f"<recipients>{recipients_xml}</recipients>"
    )
    xml = build_message("facturatie", "send_mailing", body, correlation_id=correlation_id)
    ok, msg = validate_xml(xml, "send_mailing")
    sent = publish(xml, queue="crm.incoming")
    return {"type": "send_mailing (facturatie)", "valid": ok, "sent": sent, "msg": msg}
