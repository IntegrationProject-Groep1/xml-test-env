"""
test_contract_full.py  —  Builder compliance + live routing verification
Groep 1 — Desideriushogeschool 2026  /  XML/XSD Contract v2.3

Calls actual team builder functions and validates their output against each
team's own XSD files, then optionally publishes to VM RabbitMQ to verify routing.

Usage:
    python test_contract_full.py [options]

Options:
    --host HOST         RabbitMQ host       (default: 20.126.113.148)
    --port PORT         AMQP port           (default: 30000)
    --user USER         Username            (default: guest)
    --pass PASS         Password            (default: guest)
    --mgmt-port PORT    Management API port (default: 30001)
    --vhost VHOST       Virtual host        (default: /)
    --timeout SECS      Shadow-queue wait   (default: 5)
    --teams TEAMS       kassa,planning,facturatie  (default: all)
    --repos-dir DIR     Root dir containing team repos as subdirs
    --phase1-only       Skip routing verification
    --phase2-only       Skip builder compliance
    --verbose           Print full XML payloads
    --env FILE          Load .env file (default: .env if present)
"""

import argparse
import os
import re
import sys
import textwrap
import time
import types
import uuid
from datetime import datetime, timezone
from pathlib import Path

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8")

try:
    import pika
    _PIKA_AVAILABLE = True
except ImportError:
    _PIKA_AVAILABLE = False

try:
    from lxml import etree
    _LXML_AVAILABLE = True
except ImportError:
    _LXML_AVAILABLE = False

# ── Colour helpers ─────────────────────────────────────────────────────────────
GREEN  = "\033[0;32m"
RED    = "\033[0;31m"
YELLOW = "\033[1;33m"
CYAN   = "\033[0;36m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):     print(f"  {GREEN}✓{RESET} {msg}")
def fail(msg):   print(f"  {RED}✗{RESET} {msg}")
def warn(msg):   print(f"  {YELLOW}⚠{RESET} {msg}")
def info(msg):   print(f"  {CYAN}→{RESET} {msg}")
def header(msg): print(f"\n{BOLD}{CYAN}══ {msg} ══{RESET}")
def skip(msg):   print(f"  {YELLOW}⊘{RESET} {msg}")

# ── State ──────────────────────────────────────────────────────────────────────
_state = {
    "failures": 0,
    "tests":    0,
    "results":  [],   # list of dicts written by _run_case
}

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR  = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent   # integrationProject/ when run locally

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

# ── Stable test identifiers ────────────────────────────────────────────────────
T_UUID    = "11111111-2222-3333-4444-555555555555"
T_CORR    = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
T_SESSION = "cccccccc-dddd-eeee-ffff-000000000001"
T_BADGE   = "BADGE-RF-00142"
T_NOW     = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
T_DATE    = datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ── Env / args ────────────────────────────────────────────────────────────────
def load_env(path=".env"):
    p = Path(path)
    if not p.exists():
        return
    with open(p) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def parse_args():
    p = argparse.ArgumentParser(description="Builder compliance + routing test — v2.3")
    p.add_argument("--host",        default=os.getenv("RABBIT_HOST", "20.126.113.148"))
    p.add_argument("--port",        type=int, default=int(os.getenv("RABBIT_PORT", 30000)))
    p.add_argument("--user",        default=os.getenv("RABBIT_USER", "guest"))
    p.add_argument("--pass",        dest="password", default=os.getenv("RABBIT_PASS", "guest"))
    p.add_argument("--mgmt-port",   type=int, default=int(os.getenv("RABBIT_MGMT_PORT", 30001)))
    p.add_argument("--vhost",       default=os.getenv("RABBIT_VHOST", "/"))
    p.add_argument("--timeout",     type=int, default=int(os.getenv("TIMEOUT", 5)))
    p.add_argument("--teams",       default="all")
    p.add_argument("--phase1-only", action="store_true")
    p.add_argument("--phase2-only", action="store_true")
    p.add_argument("--verbose",     action="store_true")
    p.add_argument("--env",         default=".env")
    p.add_argument(
        "--repos-dir",
        default=os.getenv("REPOS_DIR", str(PROJECT_DIR)),
        help="Root dir containing team repos as subdirs (Kassa/, Planning/, Facturatie/). "
             "In CI set to $GITHUB_WORKSPACE.",
    )
    return p.parse_args()


# ═══════════════════════════════════════════════════════════════════════════════
# Builder imports
# ═══════════════════════════════════════════════════════════════════════════════

def _stub_module(dotted_name: str, **attrs):
    """Install a minimal stub into sys.modules so a specific import doesn't fail."""
    parts = dotted_name.split(".")
    for i in range(1, len(parts) + 1):
        name = ".".join(parts[:i])
        if name not in sys.modules:
            sys.modules[name] = types.ModuleType(name)
    mod = sys.modules[dotted_name]
    for k, v in attrs.items():
        setattr(mod, k, v)
    return mod


def import_kassa_sender(repos_dir: Path):
    kassa_dir = repos_dir / "Kassa" / "integratie"
    if not kassa_dir.exists():
        return None, f"Directory not found: {kassa_dir}"

    os.environ.setdefault("RABBIT_HOST", "test-dummy")
    os.environ.setdefault("RABBIT_USER", "test-dummy")
    os.environ.setdefault("RABBIT_PASS", "test-dummy")

    if str(kassa_dir) not in sys.path:
        sys.path.insert(0, str(kassa_dir))

    try:
        import sender as kassa_sender
        return kassa_sender, None
    except Exception as e:
        return None, str(e)


def import_planning_producer(repos_dir: Path):
    planning_dir = repos_dir / "Planning"
    if not planning_dir.exists():
        return None, f"Directory not found: {planning_dir}"

    if str(planning_dir) not in sys.path:
        sys.path.insert(0, str(planning_dir))

    _stub_module("log_publisher",
                 publish_log=lambda *a, **kw: None,
                 action_for_type=lambda t: t)

    try:
        import importlib as _il
        if "producer" in sys.modules:
            producer = _il.reload(sys.modules["producer"])
        else:
            import producer
        return producer, None
    except Exception as e:
        return None, str(e)


def import_facturatie_sender(repos_dir: Path):
    fact_dir = repos_dir / "Facturatie"
    if not fact_dir.exists():
        return None, f"Directory not found: {fact_dir}"

    if str(fact_dir) not in sys.path:
        sys.path.insert(0, str(fact_dir))

    # Remove stale parent-package stubs that would block real filesystem imports.
    # We only stub the specific leaf modules that open live connections.
    for key in list(sys.modules.keys()):
        if key in ("src", "src.services", "src.utils"):
            del sys.modules[key]

    _stub_module("src.services.rabbitmq_utils",
                 get_connection=lambda: (_ for _ in ()).throw(RuntimeError("stubbed")))
    _stub_module("src.utils.xml_validator",
                 validate_xml=lambda xml, schema_name=None: (True, None))

    try:
        import importlib as _il
        if "src.services.rabbitmq_sender" in sys.modules:
            sender = _il.reload(sys.modules["src.services.rabbitmq_sender"])
        else:
            from src.services import rabbitmq_sender as sender
        return sender, None
    except Exception as e:
        return None, str(e)


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 1 — XSD + structural validation
# ═══════════════════════════════════════════════════════════════════════════════

def _load_xsd(xsd_path: Path):
    if not _LXML_AVAILABLE:
        return None, "lxml not installed"
    if not xsd_path.exists():
        return None, f"XSD not found: {xsd_path}"
    try:
        with xsd_path.open("rb") as f:
            return etree.XMLSchema(etree.parse(f)), None
    except Exception as e:
        return None, str(e)


def validate_against_xsd(xml_str: str, xsd_path: Path) -> tuple[bool, str | None]:
    schema, err = _load_xsd(xsd_path)
    if err:
        return False, err
    try:
        doc = etree.fromstring(xml_str.encode("utf-8") if isinstance(xml_str, str) else xml_str)
        if schema.validate(doc):
            return True, None
        return False, "; ".join(str(e) for e in schema.error_log)
    except etree.XMLSyntaxError as e:
        return False, f"XML syntax error: {e}"


def structural_checks(xml_str: str, expected_type: str, expected_source: str) -> list[str]:
    issues = []
    try:
        root = etree.fromstring(xml_str.encode("utf-8") if isinstance(xml_str, str) else xml_str)
    except Exception as e:
        return [f"XML parse failed: {e}"]

    for attr in root.attrib:
        if "xmlns" in attr.lower():
            issues.append(f"<message> has xmlns attribute: {attr}={root.attrib[attr]!r}")

    get = lambda tag: root.find(f"header/{tag}")
    msg_id  = get("message_id")
    src     = get("source")
    typ     = get("type")
    version = get("version")

    if msg_id is None:
        issues.append("Missing header/message_id")
    elif not UUID_RE.match(msg_id.text or ""):
        issues.append(f"header/message_id not a valid UUID: {msg_id.text!r}")
    if src is None:
        issues.append("Missing header/source")
    elif src.text != expected_source:
        issues.append(f"header/source={src.text!r}, expected {expected_source!r}")
    if typ is None:
        issues.append("Missing header/type")
    elif typ.text != expected_type:
        issues.append(f"header/type={typ.text!r}, expected {expected_type!r}")
    if version is None:
        issues.append("Missing header/version")
    elif version.text != "2.0":
        issues.append(f"header/version={version.text!r}, expected '2.0'")

    return issues


def run_phase1(name: str, xml_str: str, xsd_path: Path | None,
               expected_type: str, expected_source: str, verbose: bool) -> tuple[str | None, str]:
    """
    Run builder compliance checks.
    Returns (xml_str, status) where status is 'pass' | 'fail'.
    """
    _state["tests"] += 1

    if xml_str is None:
        fail("Builder returned None")
        return None, "fail"

    if verbose:
        info("XML output:")
        excerpt = xml_str[:1500] + ("…" if len(xml_str) > 1500 else "")
        print(textwrap.indent(excerpt, "    "))

    p1_errors = []
    issues = structural_checks(xml_str, expected_type, expected_source)
    for issue in issues:
        fail(f"Structural: {issue}")
        p1_errors.append(issue)
    if not issues:
        ok("Structural checks pass  (no xmlns, source/type/version/uuid)")

    if xsd_path:
        valid, err = validate_against_xsd(xml_str, xsd_path)
        if valid:
            ok(f"XSD valid  ({xsd_path.name})")
        else:
            fail(f"XSD invalid  ({xsd_path.name}): {err}")
            p1_errors.append(err)
    else:
        skip("No XSD for this type — structural check only")

    if p1_errors:
        _state["failures"] += 1
        return xml_str, "fail"
    return xml_str, "pass"


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 2 — Live routing
# ═══════════════════════════════════════════════════════════════════════════════

def _make_connection(args):
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=args.host, port=args.port, virtual_host=args.vhost,
        credentials=pika.PlainCredentials(args.user, args.password),
        connection_attempts=2, retry_delay=1, socket_timeout=10,
    ))


def run_phase2_topic(args, xml_str: str, exchange: str, routing_key: str,
                     exchange_type: str = "topic", label: str = "") -> str:
    _state["tests"] += 1
    shadow = f"test.shadow.{uuid.uuid4()}"
    try:
        conn = _make_connection(args)
        chan = conn.channel()
        chan.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True)
        chan.queue_declare(queue=shadow, exclusive=True, auto_delete=True)
        chan.queue_bind(queue=shadow, exchange=exchange, routing_key=routing_key)
        chan.basic_publish(
            exchange=exchange, routing_key=routing_key,
            body=xml_str.encode("utf-8"),
            properties=pika.BasicProperties(content_type="application/xml", delivery_mode=1),
        )
        deadline = time.monotonic() + args.timeout
        arrived  = False
        while time.monotonic() < deadline:
            method, _, _ = chan.basic_get(queue=shadow, auto_ack=True)
            if method:
                arrived = True
                break
            time.sleep(0.2)
        conn.close()
        tag = f"{exchange} / {routing_key}"
        if arrived:
            ok(f"Routing OK  →  {tag}")
            return "pass"
        else:
            fail(f"Not received within {args.timeout}s  →  {tag}")
            _state["failures"] += 1
            return "fail"
    except Exception as e:
        fail(f"Phase 2 error: {e}")
        _state["failures"] += 1
        return "fail"


def run_phase2_direct(args, xml_str: str, queue_name: str, label: str = "") -> str:
    _state["tests"] += 1
    try:
        conn = _make_connection(args)
        chan = conn.channel()
        chan.queue_declare(queue=queue_name, durable=True)
        chan.basic_publish(
            exchange="", routing_key=queue_name,
            body=xml_str.encode("utf-8"),
            properties=pika.BasicProperties(content_type="application/xml", delivery_mode=1),
        )
        deadline = time.monotonic() + args.timeout
        arrived  = False
        while time.monotonic() < deadline:
            method, _, _ = chan.basic_get(queue=queue_name, auto_ack=True)
            if method:
                arrived = True
                break
            time.sleep(0.2)
        conn.close()
        if arrived:
            ok(f"Routing OK  →  queue '{queue_name}'")
            return "pass"
        else:
            fail(f"Not received within {args.timeout}s  →  '{queue_name}'")
            _state["failures"] += 1
            return "fail"
    except Exception as e:
        fail(f"Phase 2 error: {e}")
        _state["failures"] += 1
        return "fail"


# ═══════════════════════════════════════════════════════════════════════════════
# Orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

def _run_case(args, name, builder_fn, xsd_path, msg_type, source,
              exch, rkey, do_p1, do_p2, direct_queue=None):
    print(f"\n  [{name}]")
    result = {"name": name, "p1": "skip", "p2": "skip", "error": ""}

    try:
        xml_str = builder_fn()
    except Exception as e:
        _state["tests"] += 1
        _state["failures"] += 1
        fail(f"Builder raised exception: {e}")
        result["p1"] = "fail"
        result["error"] = str(e)
        _state["results"].append(result)
        return

    if do_p1:
        xml_str, p1_status = run_phase1(name, xml_str, xsd_path, msg_type, source, args.verbose)
        result["p1"] = p1_status

    if do_p2 and xml_str is not None:
        if not _PIKA_AVAILABLE:
            skip("Phase 2 skipped — pika not installed")
        elif direct_queue:
            result["p2"] = run_phase2_direct(args, xml_str, direct_queue, label=name)
        else:
            result["p2"] = run_phase2_topic(args, xml_str, exch, rkey, label=name)

    _state["results"].append(result)


# ═══════════════════════════════════════════════════════════════════════════════
# Team test suites
# ═══════════════════════════════════════════════════════════════════════════════

def test_kassa(args, do_p1, do_p2):
    header("Kassa")
    repos_dir = Path(args.repos_dir)
    sender, err = import_kassa_sender(repos_dir)
    if err:
        fail(f"Cannot import Kassa sender: {err}")
        _state["results"].append({"name": "kassa/import", "p1": "fail", "p2": "skip", "error": err})
        return
    ok("Kassa sender imported")

    EXCH = "kassa.exchange"
    XSD  = repos_dir / "Kassa" / "integratie" / "schemas"
    def xsd(n): return XSD / n if (XSD / n).exists() else None

    _run_case(args, "kassa/consumption_order",
        builder_fn=lambda: sender.build_consumption_order_xml(
            items=[{"id": "1", "sku": "SKU-001", "description": "Test item",
                    "quantity": 2, "unit_price": "5.00", "vat_rate": "21",
                    "total_amount": 10.00, "currency": "eur", "item_type": "food"}],
            customer_id="42", identity_uuid=T_UUID, customer_type="private",
            email="test@example.com",
            address={"street": "Main St", "number": "1",
                     "postal_code": "1000", "city": "Brussels", "country": "be"},
        ),
        xsd_path=xsd("schema_consumption_order_v2.3.xsd"),
        msg_type="consumption_order", source="kassa",
        exch=EXCH, rkey="kassa.payments.consumption", do_p1=do_p1, do_p2=do_p2)

    _run_case(args, "kassa/payment_registered",
        # payment_method must be one of: company_link, on_site, online
        builder_fn=lambda: sender.build_payment_registered_xml(
            payment_context="consumption", invoice_status="paid",
            amount_paid="10.00", due_date=T_DATE,
            trx_id="TRX-00001", payment_method="on_site",
            invoice_id="INV-001", identity_uuid=T_UUID, correlation_id=T_CORR,
        ),
        xsd_path=xsd("schema_payment_registered_v2.1.xsd"),
        msg_type="payment_registered", source="kassa",
        exch=EXCH, rkey="kassa.payments.consumption", do_p1=do_p1, do_p2=do_p2)

    _run_case(args, "kassa/invoice_request",
        builder_fn=lambda: sender.build_invoice_request_xml(
            identity_uuid=T_UUID,
            invoice_data={"first_name": "Jan", "last_name": "Janssen",
                          "email": "jan@example.com",
                          "address": {"street": "Kerkstraat", "number": "5",
                                      "postal_code": "9000", "city": "Gent", "country": "be"}},
            correlation_id=T_CORR,
        ),
        xsd_path=xsd("schema_invoice_request.xsd"),
        msg_type="invoice_request", source="kassa",
        exch=EXCH, rkey="kassa.payments.invoice", do_p1=do_p1, do_p2=do_p2)

    _run_case(args, "kassa/badge_assigned",
        builder_fn=lambda: sender.build_badge_assigned_xml(badge_id=T_BADGE, identity_uuid=T_UUID),
        xsd_path=xsd("schema_badge_assigned.xsd"),
        msg_type="badge_assigned", source="kassa",
        exch=EXCH, rkey="kassa.payments.badge", do_p1=do_p1, do_p2=do_p2)

    _run_case(args, "kassa/wallet_balance_update",
        builder_fn=lambda: sender.build_wallet_balance_update_xml(
            identity_uuid=T_UUID, new_balance=42.50),
        xsd_path=xsd("schema_wallet_balance_update.xsd"),
        msg_type="wallet_balance_update", source="kassa",
        exch=EXCH, rkey="kassa.frontend.wallet", do_p1=do_p1, do_p2=do_p2)

    _run_case(args, "kassa/wallet_lease_request",
        builder_fn=lambda: sender.build_wallet_lease_request_xml(
            identity_uuid=T_UUID, badge_id=T_BADGE),
        xsd_path=xsd("schema_wallet_lease_request.xsd"),
        msg_type="wallet_lease_request", source="kassa",
        exch=EXCH, rkey="kassa.to.crm.wallet_lease_request", do_p1=do_p1, do_p2=do_p2)

    _run_case(args, "kassa/wallet_lease_return",
        builder_fn=lambda: sender.build_wallet_lease_return_xml(
            identity_uuid=T_UUID, final_balance=15.75, lease_id=T_CORR, transaction_count=3),
        xsd_path=xsd("schema_wallet_lease_return.xsd"),
        msg_type="wallet_lease_return", source="kassa",
        exch=EXCH, rkey="kassa.to.crm.wallet_lease_return", do_p1=do_p1, do_p2=do_p2)

    _run_case(args, "kassa/refund_processed",
        builder_fn=lambda: sender.build_refund_processed_xml(
            original_payment_msg_id=T_CORR, refund_type="consumption_item",
            refund_amount=5.00, refund_method="badge_wallet",
            refund_reason="customer_request", original_transaction_id="TRX-00001",
            identity_uuid=T_UUID,
        ),
        xsd_path=xsd("schema_refund_processed.xsd"),
        msg_type="refund_processed", source="kassa",
        exch=EXCH, rkey="kassa.payments.refund", do_p1=do_p1, do_p2=do_p2)


def test_planning(args, do_p1, do_p2):
    header("Planning")
    repos_dir = Path(args.repos_dir)
    producer, err = import_planning_producer(repos_dir)
    if err:
        fail(f"Cannot import Planning producer: {err}")
        _state["results"].append({"name": "planning/import", "p1": "fail", "p2": "skip", "error": err})
        return
    ok("Planning producer imported")

    EXCH = "planning.exchange"
    XSD  = repos_dir / "Planning" / "xsd"
    def xsd(n): return XSD / n if (XSD / n).exists() else None

    _run_case(args, "planning/session_created",
        builder_fn=lambda: producer.create_session_xml(
            session_id=T_SESSION, title="Test keynote",
            start_datetime="2026-09-01T10:00:00Z", end_datetime="2026-09-01T11:00:00Z",
            location="Aula A", max_attendees=200, current_attendees=0,
        ),
        xsd_path=xsd("session_created.xsd"),
        msg_type="session_created", source="planning",
        exch=EXCH, rkey="planning.session.created", do_p1=do_p1, do_p2=do_p2)

    _run_case(args, "planning/session_updated",
        # max_attendees is required by XSD
        builder_fn=lambda: producer.create_session_updated_xml(
            session_id=T_SESSION, title="Test keynote (updated)",
            start_datetime="2026-09-01T10:30:00Z", end_datetime="2026-09-01T11:30:00Z",
            location="Aula B", max_attendees=200,
        ),
        xsd_path=xsd("session_updated.xsd"),
        msg_type="session_updated", source="planning",
        exch=EXCH, rkey="planning.session.updated", do_p1=do_p1, do_p2=do_p2)

    _run_case(args, "planning/session_deleted",
        builder_fn=lambda: producer.create_session_deleted_xml(
            session_id=T_SESSION, reason="Cancelled by organiser", deleted_by="admin",
        ),
        xsd_path=xsd("session_deleted.xsd"),
        msg_type="session_deleted", source="planning",
        exch=EXCH, rkey="planning.session.deleted", do_p1=do_p1, do_p2=do_p2)

    # session_view_request is SENT by frontend/crm, not by planning.
    # This test validates that Planning's builder produces the correct source value.
    # Expected to fail if Planning's builder incorrectly sets source="planning".
    _run_case(args, "planning/session_view_request",
        builder_fn=lambda: producer.create_session_view_request_xml(session_id=T_SESSION),
        xsd_path=xsd("session_view_request.xsd"),
        msg_type="session_view_request", source="planning",
        exch=EXCH, rkey="planning.session.view.request", do_p1=do_p1, do_p2=do_p2)


def test_facturatie(args, do_p1, do_p2):
    header("Facturatie")
    repos_dir = Path(args.repos_dir)
    sender, err = import_facturatie_sender(repos_dir)
    if err:
        fail(f"Cannot import Facturatie sender: {err}")
        _state["results"].append({"name": "facturatie/import", "p1": "fail", "p2": "skip", "error": err})
        return
    ok("Facturatie sender imported")

    XSD = repos_dir / "Facturatie" / "src" / "services" / "xsd"
    def xsd(n): return XSD / n if (XSD / n).exists() else None

    _run_case(args, "facturatie/send_mailing",
        builder_fn=lambda: sender.build_invoice_created_notification_xml(
            invoice_id="INV-001", recipient_email="klant@example.com",
            correlation_id=T_CORR, first_name="Jan", last_name="Janssen",
            customer_id=T_UUID, subject="Uw factuur is klaar",
        ),
        xsd_path=xsd("send_mailing.xsd"),
        msg_type="send_mailing", source="facturatie",
        exch=None, rkey=None, do_p1=do_p1, do_p2=do_p2,
        direct_queue="facturatie.to.mailing")

    if hasattr(sender, "build_payment_confirmed_xml"):
        _run_case(args, "facturatie/payment_confirmed",
            builder_fn=lambda: sender.build_payment_confirmed_xml(
                invoice_id="INV-001", identity_uuid=T_UUID, amount="75.00",
                currency="eur", payment_method="card", paid_at=T_NOW,
                status="paid", due_date=T_DATE, transaction_id="TRX-001",
            ),
            xsd_path=xsd("payment_registered.xsd"),
            msg_type="payment_registered", source="facturatie",
            exch=None, rkey=None, do_p1=do_p1, do_p2=do_p2,
            direct_queue="crm.incoming")

    # Intentionally tested with defaults to surface the invalid source value
    # (default is "kassa_bar_01"; contract requires a valid source enum).
    _run_case(args, "facturatie/consumption_order",
        builder_fn=lambda: sender.build_consumption_order_xml(
            customer_id=T_UUID,
            items=[{"id": 1, "description": "Festival ticket",
                    "quantity": 1, "unit_price": "75.00", "vat_rate": "0"}],
            is_company_linked=False,
        ),
        xsd_path=xsd("consumption_order.xsd"),
        msg_type="consumption_order", source="kassa",
        exch=None, rkey=None, do_p1=do_p1, do_p2=do_p2,
        direct_queue="facturatie.incoming")


# ═══════════════════════════════════════════════════════════════════════════════
# GitHub Actions job summary
# ═══════════════════════════════════════════════════════════════════════════════

_STATUS_ICON = {"pass": "✅", "fail": "❌", "skip": "⏭️"}


def _write_gha_summary(do_p1: bool, do_p2: bool, teams_run: list[str]):
    summary_file = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_file:
        return

    results  = _state["results"]
    total    = _state["tests"]
    failures = _state["failures"]
    passed   = total - failures
    overall  = "✅ PASS" if failures == 0 else "❌ FAIL"

    with open(summary_file, "a", encoding="utf-8") as f:
        f.write("## 🧪 Builder Compliance — `test_contract_full.py`\n\n")

        # Overview table
        f.write(f"| | |\n|---|---|\n")
        f.write(f"| **Result** | {overall} |\n")
        f.write(f"| **Passed** | {passed} / {total} |\n")
        f.write(f"| **Failed** | {failures} |\n")
        f.write(f"| **Teams** | {', '.join(f'`{t}`' for t in teams_run)} |\n")
        f.write(f"| **Phase 1** | {'Builder XSD + structural validation' if do_p1 else '⏭️ skipped'} |\n")
        f.write(f"| **Phase 2** | {'Live routing via shadow queue' if do_p2 else '⏭️ skipped'} |\n\n")

        # Per-test table
        f.write("### Results per test case\n\n")
        p2_header = " | Phase 2 (Routing)" if do_p2 else ""
        f.write(f"| Test case | Phase 1 (Builder){p2_header} |\n")
        f.write(f"|---|---{' | ---' if do_p2 else ''}|\n")

        for r in results:
            p1_icon = _STATUS_ICON.get(r["p1"], "⏭️")
            row = f"| `{r['name']}` | {p1_icon} {r['p1'].upper()}"
            if do_p2:
                p2_icon = _STATUS_ICON.get(r["p2"], "⏭️")
                row += f" | {p2_icon} {r['p2'].upper()}"
            row += " |\n"
            f.write(row)

        # Failures detail
        failed = [r for r in results if r["p1"] == "fail" or r["p2"] == "fail"]
        if failed:
            f.write("\n### ⚠️ Failures\n\n")
            for r in failed:
                if r.get("error"):
                    f.write(f"**`{r['name']}`** — {r['error']}\n\n")

        f.write("\n> Run `python test_contract_full.py --verbose` locally for full XML output.\n")


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    args = parse_args()
    load_env(args.env)

    do_p1 = not args.phase2_only
    do_p2 = not args.phase1_only

    teams_arg = args.teams.lower()
    all_teams = teams_arg == "all"

    print(f"\n{BOLD}test_contract_full.py  —  Builder compliance + routing verification{RESET}")
    print(f"  Repos dir : {args.repos_dir}")
    print(f"  Phase 1   : {'YES' if do_p1 else 'SKIP'}")
    print(f"  Phase 2   : {'YES' if do_p2 else 'SKIP'}")
    if do_p2:
        print(f"  RabbitMQ  : amqp://{args.user}@{args.host}:{args.port}{args.vhost}")

    if do_p1 and not _LXML_AVAILABLE:
        warn("lxml not installed — XSD validation skipped, structural checks still run")
    if do_p2 and not _PIKA_AVAILABLE:
        warn("pika not installed — Phase 2 routing checks will be skipped")

    teams_run = []
    if all_teams or "kassa"      in teams_arg: test_kassa(args, do_p1, do_p2);      teams_run.append("kassa")
    if all_teams or "planning"   in teams_arg: test_planning(args, do_p1, do_p2);   teams_run.append("planning")
    if all_teams or "facturatie" in teams_arg: test_facturatie(args, do_p1, do_p2); teams_run.append("facturatie")

    total    = _state["tests"]
    failures = _state["failures"]
    passed   = total - failures

    print(f"\n{BOLD}{'═' * 60}{RESET}")
    if failures == 0:
        print(f"{GREEN}{BOLD}ALL {total} CHECKS PASSED{RESET}")
    else:
        print(f"{RED}{BOLD}{failures} FAILED  /  {passed} passed  (total: {total}){RESET}")

    _write_gha_summary(do_p1, do_p2, teams_run)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
