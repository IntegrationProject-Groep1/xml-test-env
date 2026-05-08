"""Kassa XML senders."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from base_sender import build_message, validate_xml, publish


def send_payment_registered(identity_uuid: str, invoice_id: str, amount_paid: str,
                              method: str = "badge_wallet", correlation_id: str = None):
    body = (
        f"<identity_uuid>{identity_uuid}</identity_uuid>"
        f"<invoice><id>{invoice_id}</id>"
        f"<amount_paid currency=\"eur\">{amount_paid}</amount_paid></invoice>"
        f"<method>{method}</method>"
    )
    xml = build_message("kassa", "payment_registered", body, correlation_id=correlation_id)
    ok, msg = validate_xml(xml, "payment_registered")
    sent = publish(xml, queue="crm.incoming")
    return {"type": "payment_registered", "valid": ok, "sent": sent, "msg": msg}


def send_system_error(error_code: str, error_description: str, related_message_id: str = None):
    body = (
        f"<error_code>{error_code}</error_code>"
        f"<error_description>{error_description}</error_description>"
    )
    if related_message_id:
        body += f"<related_message_id>{related_message_id}</related_message_id>"
    xml = build_message("kassa", "system_error", body)
    ok, msg = validate_xml(xml, "system_error")
    sent = publish(xml, queue="kassa.errors")
    return {"type": "system_error (kassa)", "valid": ok, "sent": sent, "msg": msg}


def send_wallet_balance_update(identity_uuid: str, balance: str, status: str = "active"):
    body = (
        f"<identity_uuid>{identity_uuid}</identity_uuid>"
        f"<wallet_balance currency=\"eur\">{balance}</wallet_balance>"
        f"<authority>kassa</authority>"
        f"<status>{status}</status>"
    )
    xml = build_message("kassa", "wallet_balance_update", body)
    ok, msg = validate_xml(xml, "wallet_balance_update")
    sent = publish(xml, exchange="wallet.updates", routing_key="")
    return {"type": "wallet_balance_update (kassa)", "valid": ok, "sent": sent, "msg": msg}
