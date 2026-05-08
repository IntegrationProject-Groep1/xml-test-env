"""CRM XML senders."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from base_sender import build_message, validate_xml, publish


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


def send_wallet_balance_update(identity_uuid: str, balance: str, authority: str = None):
    body = (
        f"<identity_uuid>{identity_uuid}</identity_uuid>"
        f"<wallet_balance currency=\"eur\">{balance}</wallet_balance>"
    )
    if authority:
        body += f"<authority>{authority}</authority>"
    xml = build_message("crm", "wallet_balance_update", body)
    ok, msg = validate_xml(xml, "wallet_balance_update")
    sent = publish(xml, exchange="wallet.updates", routing_key="")
    return {"type": "wallet_balance_update", "valid": ok, "sent": sent, "msg": msg}


def send_vat_validation_error(identity_uuid: str, vat_number: str, error_message: str = None):
    body = (
        f"<identity_uuid>{identity_uuid}</identity_uuid>"
        f"<vat_number>{vat_number}</vat_number>"
    )
    if error_message:
        body += f"<error_message>{error_message}</error_message>"
    xml = build_message("crm", "vat_validation_error", body)
    ok, msg = validate_xml(xml, "vat_validation_error")
    sent = publish(xml, queue="frontend.incoming")
    return {"type": "vat_validation_error", "valid": ok, "sent": sent, "msg": msg}


def send_invoice_request(identity_uuid: str, invoice_id: str, amount: str, description: str,
                         correlation_id: str = None):
    body = (
        f"<identity_uuid>{identity_uuid}</identity_uuid>"
        f"<invoice_id>{invoice_id}</invoice_id>"
        f"<amount currency=\"eur\">{amount}</amount>"
        f"<description>{description}</description>"
    )
    xml = build_message("crm", "invoice_request", body, correlation_id=correlation_id)
    ok, msg = validate_xml(xml, "invoice_request")
    sent = publish(xml, queue="facturatie.incoming")
    return {"type": "invoice_request", "valid": ok, "sent": sent, "msg": msg}


def send_wallet_remote_topup(identity_uuid: str, add_amount: str, reason: str,
                              correlation_id: str = None):
    body = (
        f"<identity_uuid>{identity_uuid}</identity_uuid>"
        f"<add_amount currency=\"eur\">{add_amount}</add_amount>"
        f"<reason>{reason}</reason>"
    )
    xml = build_message("crm", "wallet_remote_topup", body, correlation_id=correlation_id)
    ok, msg = validate_xml(xml, "wallet_remote_topup")
    sent = publish(xml, exchange="crm.exchange", routing_key="crm.to.kassa.wallet_remote_topup")
    return {"type": "wallet_remote_topup", "valid": ok, "sent": sent, "msg": msg}


def send_wallet_lease_grant(identity_uuid: str, current_balance: str, lease_id: str,
                             correlation_id: str = None):
    body = (
        f"<identity_uuid>{identity_uuid}</identity_uuid>"
        f"<current_balance currency=\"eur\">{current_balance}</current_balance>"
        f"<lease_id>{lease_id}</lease_id>"
    )
    xml = build_message("crm", "wallet_lease_grant", body, correlation_id=correlation_id)
    ok, msg = validate_xml(xml, "wallet_lease_grant")
    sent = publish(xml, exchange="crm.exchange", routing_key="crm.to.kassa.wallet_lease_grant")
    return {"type": "wallet_lease_grant", "valid": ok, "sent": sent, "msg": msg}


def send_send_mailing(campaign_id: str, subject: str, mail_type: str, recipients: list):
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
    xml = build_message("crm", "send_mailing", body)
    ok, msg = validate_xml(xml, "send_mailing")
    sent = publish(xml, queue="crm.incoming")
    return {"type": "send_mailing", "valid": ok, "sent": sent, "msg": msg}
