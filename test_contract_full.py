"""
test_contract_full.py  —  Builder compliance + live routing verification
Groep 1 — Desideriushogeschool 2026  /  XML/XSD Contract v2.3

Unlike test_integration.py (which builds its own XML), this script calls the
actual team builder functions and validates their output against each team's own
up-to-date XSD files.  It then publishes the exact same XML to the VM and
verifies routing via shadow queues.

Phase 1  Builder compliance
  · Import actual team builder functions (Kassa, Planning, Facturatie)
  · Build XML with representative test data
  · Validate against team's local XSD files (authoritative, up-to-date)
  · Structural checks: no xmlns on <message>, source, version, UUID format

Phase 2  Live routing verification
  · Publish builder-produced XML to VM RabbitMQ
  · Shadow queue: temporary exclusive queue bound to the expected
    exchange + routing key — verify message arrives via basic_get
  · Facturatie (default exchange): direct queue check

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
    --phase1-only       Skip routing verification
    --phase2-only       Skip builder compliance
    --verbose           Print full XML payloads
    --env FILE          Load .env file      (default: .env if present)
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
def fail(msg):   print(f"  {RED}✗{RESET} {msg}"); _state["failures"] += 1
def warn(msg):   print(f"  {YELLOW}⚠{RESET} {msg}")
def info(msg):   print(f"  {CYAN}→{RESET} {msg}")
def header(msg): print(f"\n{BOLD}{CYAN}══ {msg} ══{RESET}")
def skip(msg):   print(f"  {YELLOW}⊘{RESET} {msg}")

_state = {"failures": 0, "tests": 0}


# ── Paths ──────────────────────────────────────────────────────────────────────
# This script lives in xml-test-env/ which is a direct child of the project root.
SCRIPT_DIR  = Path(__file__).resolve().parent   # integrationProject/xml-test-env
PROJECT_DIR = SCRIPT_DIR.parent                 # integrationProject

# Team XSD directories — each team maintains their own up-to-date schemas
KASSA_XSD_DIR    = PROJECT_DIR / "Kassa"      / "integratie" / "schemas"
PLANNING_XSD_DIR = PROJECT_DIR / "Planning"   / "xsd"
FACT_XSD_DIR     = PROJECT_DIR / "Facturatie" / "src" / "services" / "xsd"

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


# ── Env loader ────────────────────────────────────────────────────────────────
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
    p = argparse.ArgumentParser(description="Builder compliance + routing test — Groep 1 v2.3")
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
        help=(
            "Root directory containing team repos as subdirectories. "
            "Locally this is the integrationProject/ folder. "
            "In CI set it to $GITHUB_WORKSPACE where each repo is checked out "
            "as a named subdirectory (e.g. kassa/, planning/, facturatie/)."
        ),
    )
    return p.parse_args()


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1  —  Builder import helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _stub_module(dotted_name: str, **attrs):
    """Install a minimal stub into sys.modules so imports don't fail."""
    parts = dotted_name.split(".")
    for i in range(1, len(parts) + 1):
        name = ".".join(parts[:i])
        if name not in sys.modules:
            sys.modules[name] = types.ModuleType(name)
    mod = sys.modules[dotted_name]
    for k, v in attrs.items():
        setattr(mod, k, v)
    return mod


def import_kassa_sender(repos_dir: Path = PROJECT_DIR):
    """Import Kassa sender with dummy env so require_env() doesn't abort."""
    kassa_dir = repos_dir / "Kassa" / "integratie"
    if not kassa_dir.exists():
        return None, f"Directory not found: {kassa_dir}"

    # require_env() reads from os.environ at module load time — set dummies first
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


def import_planning_producer(repos_dir: Path = PROJECT_DIR):
    """Import Planning producer. Stubs out log_publisher (needs live RabbitMQ)."""
    planning_dir = repos_dir / "Planning"
    if not planning_dir.exists():
        return None, f"Directory not found: {planning_dir}"

    if str(planning_dir) not in sys.path:
        sys.path.insert(0, str(planning_dir))

    # log_publisher connects to RabbitMQ at import time — stub it
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


def import_facturatie_sender(repos_dir: Path = PROJECT_DIR):
    """Import Facturatie sender with stubs for connection helpers."""
    fact_dir = repos_dir / "Facturatie"
    if not fact_dir.exists():
        return None, f"Directory not found: {fact_dir}"

    if str(fact_dir) not in sys.path:
        sys.path.insert(0, str(fact_dir))

    # Stub internal dependencies that open live connections
    _stub_module("src")
    _stub_module("src.services")
    _stub_module("src.utils")
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
# PHASE 1  —  XSD + structural validation
# ═══════════════════════════════════════════════════════════════════════════════

def _load_xsd(xsd_path: Path):
    """Load an XSD from an absolute path.  Returns (XMLSchema, None) or (None, err)."""
    if not _LXML_AVAILABLE:
        return None, "lxml not installed"
    if not xsd_path.exists():
        return None, f"XSD not found: {xsd_path}"
    try:
        with xsd_path.open("rb") as f:
            doc = etree.parse(f)
        return etree.XMLSchema(doc), None
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
        errs = "; ".join(str(e) for e in schema.error_log)
        return False, errs
    except etree.XMLSyntaxError as e:
        return False, f"XML syntax error: {e}"


def structural_checks(xml_str: str, expected_type: str, expected_source: str) -> list[str]:
    """Return list of violation strings (empty = all OK)."""
    issues = []
    try:
        root = etree.fromstring(xml_str.encode("utf-8") if isinstance(xml_str, str) else xml_str)
    except Exception as e:
        return [f"XML parse failed: {e}"]

    # Contract §23: <message> must have NO xmlns attributes
    for attr in root.attrib:
        if "xmlns" in attr.lower():
            issues.append(f"<message> has xmlns attribute: {attr}={root.attrib[attr]!r}")

    # Standard header field checks
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
               expected_type: str, expected_source: str, verbose: bool) -> str | None:
    """Run builder compliance checks.  Returns xml_str so Phase 2 can reuse it."""
    _state["tests"] += 1
    print(f"\n  [{name}]")

    if xml_str is None:
        fail("Builder returned None")
        return None

    if verbose:
        info("XML output:")
        excerpt = xml_str[:1500] + ("…" if len(xml_str) > 1500 else "")
        print(textwrap.indent(excerpt, "    "))

    issues = structural_checks(xml_str, expected_type, expected_source)
    if issues:
        for issue in issues:
            fail(f"Structural: {issue}")
    else:
        ok("Structural checks pass  (no xmlns, source/type/version/uuid)")

    if xsd_path:
        valid, err = validate_against_xsd(xml_str, xsd_path)
        if valid:
            ok(f"XSD valid  ({xsd_path.name})")
        else:
            fail(f"XSD invalid  ({xsd_path.name}): {err}")
    else:
        skip("No XSD configured for this message type — structural check only")

    return xml_str


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2  —  Live routing via shadow queue / direct queue
# ═══════════════════════════════════════════════════════════════════════════════

def _make_connection(args):
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=args.host,
        port=args.port,
        virtual_host=args.vhost,
        credentials=pika.PlainCredentials(args.user, args.password),
        connection_attempts=2,
        retry_delay=1,
        socket_timeout=10,
    ))


def run_phase2_topic(args, xml_str: str, exchange: str, routing_key: str,
                     exchange_type: str = "topic", label: str = ""):
    """
    Publish to a topic exchange and verify arrival via a temporary shadow queue.
    The shadow queue is exclusive + auto-delete, so it has no production impact.
    """
    _state["tests"] += 1
    shadow = f"test.shadow.{uuid.uuid4()}"
    try:
        conn = _make_connection(args)
        chan = conn.channel()

        chan.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True)
        chan.queue_declare(queue=shadow, exclusive=True, auto_delete=True)
        chan.queue_bind(queue=shadow, exchange=exchange, routing_key=routing_key)

        chan.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
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
            ok(f"Routing OK  →  {tag}  ({label})")
        else:
            fail(f"Message NOT received within {args.timeout}s  →  {tag}  ({label})")

    except Exception as e:
        fail(f"Phase 2 error  ({label}): {e}")


def run_phase2_direct(args, xml_str: str, queue_name: str, label: str = ""):
    """
    Publish to the default exchange (direct queue routing) and confirm arrival.
    Message is immediately ack'd so real consumers are unaffected.
    """
    _state["tests"] += 1
    try:
        conn = _make_connection(args)
        chan = conn.channel()

        chan.queue_declare(queue=queue_name, durable=True)
        chan.basic_publish(
            exchange="",
            routing_key=queue_name,
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
            ok(f"Routing OK  →  direct queue '{queue_name}'  ({label})")
        else:
            fail(f"Message NOT received within {args.timeout}s  →  '{queue_name}'  ({label})")

    except Exception as e:
        fail(f"Phase 2 error  ({label}): {e}")


def _run_case(args, name, builder_fn, xsd_path, msg_type, source,
              exch, rkey, do_p1, do_p2, direct_queue=None):
    """Execute one test case: build → validate → route."""
    try:
        xml_str = builder_fn()
    except Exception as e:
        _state["tests"] += 1
        print(f"\n  [{name}]")
        fail(f"Builder raised exception: {e}")
        return

    if do_p1:
        xml_str = run_phase1(name, xml_str, xsd_path, msg_type, source, args.verbose)

    if not do_p2 or xml_str is None:
        return

    if not _PIKA_AVAILABLE:
        skip(f"[{name}] Phase 2 skipped — pika not installed")
        return

    if direct_queue:
        run_phase2_direct(args, xml_str, direct_queue, label=name)
    else:
        run_phase2_topic(args, xml_str, exch, rkey, label=name)


# ═══════════════════════════════════════════════════════════════════════════════
# TEAM TEST SUITES
# ═══════════════════════════════════════════════════════════════════════════════

def test_kassa(args, do_p1, do_p2):
    header("Kassa")

    repos_dir = Path(args.repos_dir)
    sender, err = import_kassa_sender(repos_dir)
    if err:
        fail(f"Cannot import Kassa sender: {err}")
        return
    ok("Kassa sender imported")

    EXCH = "kassa.exchange"
    XSD  = repos_dir / "Kassa" / "integratie" / "schemas"

    def xsd(name): return XSD / name if (XSD / name).exists() else None

    # ── consumption_order ──────────────────────────────────────────────────────
    _run_case(args, "kassa/consumption_order",
        builder_fn=lambda: sender.build_consumption_order_xml(
            items=[{
                "id": "1", "sku": "SKU-001", "description": "Test item",
                "quantity": 2, "unit_price": "5.00", "vat_rate": "21",
                "total_amount": 10.00, "currency": "eur", "item_type": "food",
            }],
            customer_id="42", identity_uuid=T_UUID,
            customer_type="private", email="test@example.com",
            address={"street": "Main St", "number": "1",
                     "postal_code": "1000", "city": "Brussels", "country": "be"},
        ),
        xsd_path=xsd("schema_consumption_order_v2.3.xsd"),
        msg_type="consumption_order", source="kassa",
        exch=EXCH, rkey="kassa.payments.consumption",
        do_p1=do_p1, do_p2=do_p2,
    )

    # ── payment_registered ─────────────────────────────────────────────────────
    _run_case(args, "kassa/payment_registered",
        builder_fn=lambda: sender.build_payment_registered_xml(
            payment_context="consumption", invoice_status="paid",
            amount_paid="10.00", due_date=T_DATE,
            trx_id="TRX-00001", payment_method="card",
            invoice_id="INV-001", identity_uuid=T_UUID, correlation_id=T_CORR,
        ),
        xsd_path=xsd("schema_payment_registered_v2.1.xsd"),
        msg_type="payment_registered", source="kassa",
        exch=EXCH, rkey="kassa.payments.consumption",
        do_p1=do_p1, do_p2=do_p2,
    )

    # ── invoice_request ────────────────────────────────────────────────────────
    _run_case(args, "kassa/invoice_request",
        builder_fn=lambda: sender.build_invoice_request_xml(
            identity_uuid=T_UUID,
            invoice_data={
                "first_name": "Jan", "last_name": "Janssen",
                "email": "jan@example.com",
                "address": {"street": "Kerkstraat", "number": "5",
                            "postal_code": "9000", "city": "Gent", "country": "be"},
            },
            correlation_id=T_CORR,
        ),
        xsd_path=xsd("schema_invoice_request.xsd"),
        msg_type="invoice_request", source="kassa",
        exch=EXCH, rkey="kassa.payments.invoice",
        do_p1=do_p1, do_p2=do_p2,
    )

    # ── badge_assigned ─────────────────────────────────────────────────────────
    _run_case(args, "kassa/badge_assigned",
        builder_fn=lambda: sender.build_badge_assigned_xml(
            badge_id=T_BADGE, identity_uuid=T_UUID),
        xsd_path=xsd("schema_badge_assigned.xsd"),
        msg_type="badge_assigned", source="kassa",
        exch=EXCH, rkey="kassa.payments.badge",
        do_p1=do_p1, do_p2=do_p2,
    )

    # ── wallet_balance_update ──────────────────────────────────────────────────
    _run_case(args, "kassa/wallet_balance_update",
        builder_fn=lambda: sender.build_wallet_balance_update_xml(
            identity_uuid=T_UUID, new_balance=42.50),
        xsd_path=xsd("schema_wallet_balance_update.xsd"),
        msg_type="wallet_balance_update", source="kassa",
        exch=EXCH, rkey="kassa.frontend.wallet",
        do_p1=do_p1, do_p2=do_p2,
    )

    # ── wallet_lease_request ───────────────────────────────────────────────────
    _run_case(args, "kassa/wallet_lease_request",
        builder_fn=lambda: sender.build_wallet_lease_request_xml(
            identity_uuid=T_UUID, badge_id=T_BADGE),
        xsd_path=xsd("schema_wallet_lease_request.xsd"),
        msg_type="wallet_lease_request", source="kassa",
        exch=EXCH, rkey="kassa.to.crm.wallet_lease_request",
        do_p1=do_p1, do_p2=do_p2,
    )

    # ── wallet_lease_return ────────────────────────────────────────────────────
    _run_case(args, "kassa/wallet_lease_return",
        builder_fn=lambda: sender.build_wallet_lease_return_xml(
            identity_uuid=T_UUID, final_balance=15.75,
            lease_id=T_CORR, transaction_count=3),
        xsd_path=xsd("schema_wallet_lease_return.xsd"),
        msg_type="wallet_lease_return", source="kassa",
        exch=EXCH, rkey="kassa.to.crm.wallet_lease_return",
        do_p1=do_p1, do_p2=do_p2,
    )

    # ── refund_processed ──────────────────────────────────────────────────────
    _run_case(args, "kassa/refund_processed",
        builder_fn=lambda: sender.build_refund_processed_xml(
            original_payment_msg_id=T_CORR,
            refund_type="consumption_item",
            refund_amount=5.00, refund_method="badge_wallet",
            refund_reason="customer_request",
            original_transaction_id="TRX-00001",
            identity_uuid=T_UUID,
        ),
        xsd_path=xsd("schema_refund_processed.xsd"),
        msg_type="refund_processed", source="kassa",
        exch=EXCH, rkey="kassa.payments.refund",
        do_p1=do_p1, do_p2=do_p2,
    )


def test_planning(args, do_p1, do_p2):
    header("Planning")

    repos_dir = Path(args.repos_dir)
    producer, err = import_planning_producer(repos_dir)
    if err:
        fail(f"Cannot import Planning producer: {err}")
        return
    ok("Planning producer imported")

    EXCH = "planning.exchange"
    XSD  = repos_dir / "Planning" / "xsd"

    def xsd(name): return XSD / name if (XSD / name).exists() else None

    # ── session_created ────────────────────────────────────────────────────────
    _run_case(args, "planning/session_created",
        builder_fn=lambda: producer.create_session_xml(
            session_id=T_SESSION,
            title="Test keynote",
            start_datetime="2026-09-01T10:00:00Z",
            end_datetime="2026-09-01T11:00:00Z",
            location="Aula A",
            max_attendees=200,
            current_attendees=0,
        ),
        xsd_path=xsd("session_created.xsd"),
        msg_type="session_created", source="planning",
        exch=EXCH, rkey="planning.session.created",
        do_p1=do_p1, do_p2=do_p2,
    )

    # ── session_updated ────────────────────────────────────────────────────────
    _run_case(args, "planning/session_updated",
        builder_fn=lambda: producer.create_session_updated_xml(
            session_id=T_SESSION,
            title="Test keynote (updated)",
            start_datetime="2026-09-01T10:30:00Z",
            end_datetime="2026-09-01T11:30:00Z",
            location="Aula B",
        ),
        xsd_path=xsd("session_updated.xsd"),
        msg_type="session_updated", source="planning",
        exch=EXCH, rkey="planning.session.updated",
        do_p1=do_p1, do_p2=do_p2,
    )

    # ── session_deleted ────────────────────────────────────────────────────────
    _run_case(args, "planning/session_deleted",
        builder_fn=lambda: producer.create_session_deleted_xml(
            session_id=T_SESSION,
            reason="Cancelled by organiser",
            deleted_by="admin",
        ),
        xsd_path=xsd("session_deleted.xsd"),
        msg_type="session_deleted", source="planning",
        exch=EXCH, rkey="planning.session.deleted",
        do_p1=do_p1, do_p2=do_p2,
    )

    # ── session_view_request ───────────────────────────────────────────────────
    _run_case(args, "planning/session_view_request",
        builder_fn=lambda: producer.create_session_view_request_xml(
            session_id=T_SESSION),
        xsd_path=xsd("session_view_request.xsd"),
        msg_type="session_view_request", source="planning",
        exch=EXCH, rkey="planning.session.view.request",
        do_p1=do_p1, do_p2=do_p2,
    )


def test_facturatie(args, do_p1, do_p2):
    header("Facturatie")

    repos_dir = Path(args.repos_dir)
    sender, err = import_facturatie_sender(repos_dir)
    if err:
        fail(f"Cannot import Facturatie sender: {err}")
        return
    ok("Facturatie sender imported")

    XSD = repos_dir / "Facturatie" / "src" / "services" / "xsd"

    def xsd(name): return XSD / name if (XSD / name).exists() else None

    # ── send_mailing (invoice created notification) ────────────────────────────
    _run_case(args, "facturatie/send_mailing",
        builder_fn=lambda: sender.build_invoice_created_notification_xml(
            invoice_id="INV-001",
            recipient_email="klant@example.com",
            correlation_id=T_CORR,
            first_name="Jan",
            last_name="Janssen",
            customer_id=T_UUID,
            subject="Uw factuur is klaar",
        ),
        xsd_path=xsd("send_mailing.xsd"),
        msg_type="send_mailing", source="facturatie",
        exch=None, rkey=None,
        do_p1=do_p1, do_p2=do_p2,
        direct_queue="facturatie.to.mailing",
    )

    # ── payment_confirmed (payment_registered) ─────────────────────────────────
    if hasattr(sender, "build_payment_confirmed_xml"):
        _run_case(args, "facturatie/payment_confirmed",
            builder_fn=lambda: sender.build_payment_confirmed_xml(
                invoice_id="INV-001",
                identity_uuid=T_UUID,
                amount="75.00",
                currency="eur",
                payment_method="card",
                paid_at=T_NOW,
                status="paid",
                due_date=T_DATE,
                transaction_id="TRX-001",
            ),
            xsd_path=xsd("payment_registered.xsd"),
            msg_type="payment_registered", source="facturatie",
            exch=None, rkey=None,
            do_p1=do_p1, do_p2=do_p2,
            direct_queue="crm.incoming",
        )

    # ── consumption_order (Facturatie's own builder) ───────────────────────────
    # Intentionally tested with defaults to surface the invalid source value
    # (default is "kassa_bar_01" but contract requires a valid source enum).
    _run_case(args, "facturatie/consumption_order",
        builder_fn=lambda: sender.build_consumption_order_xml(
            customer_id=T_UUID,
            items=[{
                "id": 1, "description": "Festival ticket",
                "quantity": 1, "unit_price": "75.00", "vat_rate": "0",
            }],
            is_company_linked=False,
        ),
        xsd_path=xsd("consumption_order.xsd"),
        msg_type="consumption_order", source="kassa",  # contract expects source="kassa"
        exch=None, rkey=None,
        do_p1=do_p1, do_p2=do_p2,
        direct_queue="facturatie.incoming",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def _write_gha_summary(total: int, failures: int, teams_run: list[str]):
    """Write a markdown summary to $GITHUB_STEP_SUMMARY when running in GitHub Actions."""
    summary_file = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_file:
        return
    passed = total - failures
    icon   = "✅" if failures == 0 else "❌"
    with open(summary_file, "a", encoding="utf-8") as f:
        f.write("## Builder Compliance — `test_contract_full.py`\n\n")
        f.write(f"| Result | Checks passed | Checks failed |\n")
        f.write(f"|---|---|---|\n")
        f.write(f"| {icon} {'PASS' if failures == 0 else 'FAIL'} | {passed} | {failures} |\n\n")
        f.write(f"**Teams tested:** {', '.join(teams_run)}\n\n")
        if failures > 0:
            f.write("> ⚠️ One or more builders produced XML that fails XSD validation or "
                    "violates contract structural rules. Check the step log for details.\n\n")
        f.write("| Phase | Description |\n")
        f.write("|---|---|\n")
        f.write("| Phase 1 | Import actual team builder functions, build XML, "
                "validate against team XSD + structural rules |\n")
        f.write("| Phase 2 | Publish builder-produced XML to RabbitMQ, "
                "verify routing via shadow queue |\n")


def main():
    args = parse_args()
    load_env(args.env)

    do_p1 = not args.phase2_only
    do_p2 = not args.phase1_only

    teams_arg = args.teams.lower()
    all_teams = teams_arg == "all"
    repos_dir = Path(args.repos_dir)

    print(f"\n{BOLD}test_contract_full.py  —  Builder compliance + routing verification{RESET}")
    print(f"  Repos dir    : {repos_dir}")
    print(f"  Phase 1 (builder compliance)  : {'YES' if do_p1 else 'SKIP'}")
    print(f"  Phase 2 (live routing)         : {'YES' if do_p2 else 'SKIP'}")
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

    # ── Summary ────────────────────────────────────────────────────────────────
    total    = _state["tests"]
    failures = _state["failures"]
    passed   = total - failures
    print(f"\n{BOLD}{'═' * 60}{RESET}")
    if failures == 0:
        print(f"{GREEN}{BOLD}ALL {total} CHECKS PASSED{RESET}")
    else:
        print(f"{RED}{BOLD}{failures} FAILED  /  {passed} passed  (total: {total}){RESET}")

    _write_gha_summary(total, failures, teams_run)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
