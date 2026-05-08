"""
Run all XML test senders and report validation + publish results.

Usage:
    python run_all.py           # sends all messages, validates XSD, publishes to RabbitMQ
    python run_all.py --dry-run # only validates XML/XSD, no RabbitMQ connection needed
"""
import sys
import importlib
import uuid
from pathlib import Path

DRY_RUN = "--dry-run" in sys.argv

sys.path.insert(0, str(Path(__file__).parent / "senders"))

if not DRY_RUN:
    import pika
    from senders.base_sender import publish as _orig_publish
else:
    def _patch_publish():
        import senders.base_sender as bs
        bs.publish = lambda *a, **kw: True
    _patch_publish()

from senders.frontend_sender import (
    send_new_registration, send_user_created, send_user_registered,
    send_event_ended, send_calendar_invite,
)
from senders.crm_sender import (
    send_wallet_balance_update, send_vat_validation_error,
    send_wallet_remote_topup, send_wallet_lease_grant, send_send_mailing,
)
from senders.planning_sender import send_session_created, send_session_occupancy_update
from senders.facturatie_sender import (
    send_invoice_available, send_send_mailing as facturatie_send_mailing,
)
from senders.kassa_sender import (
    send_payment_registered, send_system_error, send_wallet_balance_update as kassa_wallet_update,
)


TEST_UUID = "e8b27c1d-4f2a-4b3e-9c5f-123456789abc"
SESSION_ID = "sess-test-001"
CORR_ID = str(uuid.uuid4())

TESTS = [
    lambda: send_new_registration(TEST_UUID, "jan@test.be", "Jan", "Peeters", "1995-03-21"),
    lambda: send_user_created(TEST_UUID, "jan@test.be", "Jan", "Peeters", "1995-03-21"),
    lambda: send_user_registered(TEST_UUID, "jan@test.be", "Jan", "Peeters", SESSION_ID),
    lambda: send_event_ended(SESSION_ID, "2026-05-15T22:00:00Z"),
    lambda: send_calendar_invite(SESSION_ID, "Test Sessie", "2026-05-15T14:00:00Z", "2026-05-15T15:00:00Z", "online"),
    lambda: send_session_created(SESSION_ID, "Test Sessie", "2026-05-15T14:00:00Z", "2026-05-15T15:00:00Z", "online"),
    lambda: send_session_occupancy_update(SESSION_ID, 50, 120, "available"),
    lambda: send_wallet_balance_update(TEST_UUID, "50.00"),
    lambda: send_vat_validation_error(TEST_UUID, "BE0000000000", "BTW-nummer ongeldig"),
    lambda: send_wallet_remote_topup(TEST_UUID, "20.00", "online_topup", correlation_id=CORR_ID),
    lambda: send_wallet_lease_grant(TEST_UUID, "50.00", "LEASE-2026-001", correlation_id=CORR_ID),
    lambda: send_send_mailing("CAMP-001", "Bevestiging", "registration_confirmation",
                               [{"email": "jan@test.be", "user_id": TEST_UUID,
                                 "first_name": "Jan", "last_name": "Peeters"}]),
    lambda: send_invoice_available(TEST_UUID, "INV-142", "https://facturatie.example.com/invoice/142"),
    lambda: facturatie_send_mailing("CAMP-002", "Factuur beschikbaar", "invoice_notification",
                                    [{"email": "jan@test.be", "user_id": TEST_UUID,
                                      "first_name": "Jan", "last_name": "Peeters"}]),
    lambda: send_payment_registered(TEST_UUID, "INV-001", "25.00"),
    lambda: send_system_error("database_error", "DB connection timeout"),
    lambda: kassa_wallet_update(TEST_UUID, "30.00"),
]

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"

results = []
for test in TESTS:
    try:
        r = test()
        status = PASS if r["valid"] and r["sent"] else FAIL
        detail = "" if r["valid"] else f"  XSD: {r['msg']}"
        print(f"  [{status}] {r['type']}{detail}")
        results.append(r)
    except Exception as e:
        print(f"  [{FAIL}] ERROR: {e}")
        results.append({"valid": False, "sent": False})

total = len(results)
passed = sum(1 for r in results if r.get("valid") and r.get("sent"))
print(f"\n{'='*50}")
print(f"  Result: {passed}/{total} passed")
if DRY_RUN:
    print("  (dry-run: no RabbitMQ messages were sent)")
