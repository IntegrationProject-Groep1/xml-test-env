"""
test_contract_full.py  —  Comprehensive Behavioral Contract Audit
Groep 1 — Desideriushogeschool 2026  /  XML/XSD Contract v2.3

This suite executes real team code to verify:
1. BUILD: Can the sender generate valid XML? (Phase 1)
2. PROCESS: Can the receiver handle the XML without logic errors? (Phase 3)

Phase 2 (RabbitMQ Routing) is handled by test_integration.py.
"""

import argparse
import os
import re
import sys
import textwrap
import time
import types
import uuid
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

try:
    from lxml import etree
    _LXML_AVAILABLE = True
except ImportError:
    _LXML_AVAILABLE = False

# ── Style helpers ──────────────────────────────────────────────────────────────
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

# ── Global State ───────────────────────────────────────────────────────────────
_state = {"failures": 0, "tests": 0, "results": []}
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

# ── Stable Test Data ───────────────────────────────────────────────────────────
T_UUID    = "11111111-2222-3333-4444-555555555555"
T_CORR    = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
T_SESSION = "cccccccc-dddd-eeee-ffff-000000000001"
T_BADGE   = "BADGE-RF-00142"
T_DATE    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
T_NOW     = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ── Dependency Stubbing ────────────────────────────────────────────────────────
def _stub_module(name: str, **attrs):
    parts = name.split(".")
    for i in range(1, len(parts) + 1):
        n = ".".join(parts[:i])
        if n not in sys.modules:
            m = types.ModuleType(n)
            if i < len(parts): m.__path__ = [] 
            sys.modules[n] = m
            if i > 1:
                parent = ".".join(parts[:i-1])
                setattr(sys.modules[parent], parts[i-1], sys.modules[n])
    mod = sys.modules[name]
    for k, v in attrs.items(): setattr(mod, k, v)
    return mod

def load_env(path=".env"):
    p = Path(path)
    if not p.exists(): return
    with open(p) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--teams", default="all")
    p.add_argument("--repos-dir", default=os.getenv("REPOS_DIR", str(Path(__file__).resolve().parent.parent)))
    p.add_argument("--phase1-only", action="store_true", help="Compatibility")
    p.add_argument("--phase2-only", action="store_true", help="Compatibility")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--env", default=".env")
    return p.parse_args()

# ═══════════════════════════════════════════════════════════════════════════════
# Validation Engine
# ═══════════════════════════════════════════════════════════════════════════════

def validate_against_xsd(xml_str: str, xsd_path: Path) -> tuple[bool, str | None]:
    if not _LXML_AVAILABLE: return False, "lxml missing"
    if not xsd_path or not xsd_path.exists(): return False, f"XSD missing: {xsd_path}"
    try:
        with xsd_path.open("rb") as f: schema = etree.XMLSchema(etree.parse(f))
        doc = etree.fromstring(xml_str.encode("utf-8") if isinstance(xml_str, str) else xml_str)
        if schema.validate(doc): return True, None
        return False, "; ".join(str(e) for e in schema.error_log)
    except Exception as e: return False, str(e)

def structural_checks(xml_str: str, expected_type: str, expected_source: str, flat_root: str | None = None) -> list[str]:
    issues = []
    try:
        root = etree.fromstring(xml_str.encode("utf-8") if isinstance(xml_str, str) else xml_str)
    except Exception as e: return [f"XML parse failed: {e}"]
    
    if not flat_root:
        get = lambda tag: root.find(f"header/{tag}")
        mid, src, typ, ver = get("message_id"), get("source"), get("type"), get("version")
        if mid is None: issues.append("Missing <header/message_id>")
        elif not UUID_RE.match((mid.text or "").strip()): issues.append(f"Invalid UUID: {mid.text}")
        if src is None: issues.append("Missing <header/source>")
        elif (src.text or "").strip() != expected_source: issues.append(f"Source mismatch: got {src.text}, want {expected_source}")
        if typ is None: issues.append("Missing <header/type>")
        elif (typ.text or "").strip() != expected_type: issues.append(f"Type mismatch: got {typ.text}, want {expected_type}")
        if ver is None: issues.append("Missing <header/version>")
        elif (ver.text or "").strip() != "2.0": issues.append(f"Version mismatch: got {ver.text}, want 2.0")
    elif root.tag != flat_root:
        issues.append(f"Root mismatch: got <{root.tag}>, want <{flat_root}>")
    return issues

def _run_case(args, name, builder_fn, xsd_path, msg_type, source, receiver_fns=None, flat_root=None):
    print(f"\n  [{name}]")
    _state["tests"] += 1
    res = {"name": name, "p1": "fail", "p3": "skip", "error": "", "proc_error": ""}
    
    try:
        xml_str = builder_fn()
        if xml_str is None: raise ValueError("Builder returned None")
        if isinstance(xml_str, bytes): xml_str = xml_str.decode("utf-8")
        if args.verbose: print(textwrap.indent(xml_str[:1500], "    "))
        
        issues = structural_checks(xml_str, msg_type, source, flat_root)
        if issues:
            for i in issues: fail(f"Structural: {i}")
            res["error"] = "; ".join(issues)
        else:
            ok("Phase 1: Structural pass")
            valid, err = validate_against_xsd(xml_str, xsd_path)
            if valid:
                ok(f"Phase 1: XSD valid ({xsd_path.name if xsd_path else 'no XSD'})")
                res["p1"] = "pass"
            else:
                fail(f"Phase 1: XSD invalid: {err}")
                res["error"] = err
    except Exception as e:
        fail(f"Phase 1: Builder error: {e}")
        res["error"] = str(e)
        xml_str = None

    if not args.phase1_only and receiver_fns and xml_str:
        for receiver_name, receiver_fn in receiver_fns.items():
            _state["tests"] += 1
            info(f"Phase 3: Injecting into handler: {receiver_name}...")
            try:
                clean_xml = re.sub(r'<\?xml[^?]+\?>', '', xml_str).strip()
                proc_res, proc_err = receiver_fn(clean_xml.encode('utf-8'))
                if proc_res:
                    ok(f"Phase 3 ({receiver_name}): Processing success")
                    if res["p3"] == "skip": res["p3"] = "pass"
                else:
                    fail(f"Phase 3 ({receiver_name}): Processing failure: {proc_err}")
                    res["p3"] = "fail"
                    res["proc_error"] += f"[{receiver_name}]: {proc_err}; "
            except Exception as e:
                fail(f"Phase 3 ({receiver_name}): Handler crash: {e}")
                res["p3"] = "fail"
                res["proc_error"] += f"[{receiver_name}]: {e}; "

    if res["p1"] == "fail" or res["p3"] == "fail": _state["failures"] += 1
    _state["results"].append(res)

# ── Repository path helper ─────────────────────────────────────────────────────
def find_repo(repos_dir: Path, name: str) -> Path | None:
    options = [repos_dir / name, repos_dir.parent / name, repos_dir / "xml-test-env" / name]
    for p in options:
        if p.exists() and p.is_dir(): return p
    return None

# ═══════════════════════════════════════════════════════════════════════════════
# Kassa Suite
# ═══════════════════════════════════════════════════════════════════════════════

def test_kassa(args):
    header("Kassa Audit (POS Integration)")
    repos = Path(args.repos_dir); k_dir = find_repo(repos, "Kassa")
    if not k_dir: fail("Kassa repo not found"); return
    ki = k_dir / "integratie"
    
    mock_env = {"RABBIT_HOST":"l","RABBIT_USER":"g","RABBIT_PASS":"g","ODOO_URL":"u","ODOO_DB":"d","ODOO_USER":"u","ODOO_PASS":"p"}
    with patch.dict(os.environ, mock_env):
        sys.path.insert(0, str(ki))
        _stub_module("defusedxml.ElementTree", fromstring=etree.fromstring)
        _stub_module("defusedxml.xmlrpc", monkeypatch=MagicMock())
        _stub_module("defusedxml.xmlrpc.monkeypatch", monkey_patch=MagicMock())
        try:
            for m in ["sender","receiver"]: 
                if m in sys.modules: del sys.modules[m]
            import sender as s; import receiver as r
            xsd = lambda n: ki / "schemas" / n
            def k_rec(b):
                ch = MagicMock()
                with patch("receiver.get_odoo_connection", return_value=(1, MagicMock())):
                    r.process_message(ch, MagicMock(delivery_tag=1), MagicMock(), b)
                return ch.basic_ack.called, "Nacked"

            rec = {"kassa": k_rec}
            _run_case(args, "kassa/consumption_order", lambda: s.build_consumption_order_xml([{"id":"1","sku":"S1","description":"T","quantity":1,"unit_price":"5.0","vat_rate":"21","total_amount":5.0,"currency":"eur","item_type":"food"}], "42", T_UUID, "private", "t@e.com", {"street":"S","number":"1","postal_code":"1","city":"B","country":"be"}), xsd("schema_consumption_order_v2.3.xsd"), "consumption_order", "kassa", receiver_fns=rec)
            _run_case(args, "kassa/payment_registered", lambda: s.build_payment_registered_xml("consumption", "paid", "10.0", T_DATE, "T1", "on_site", "I1", T_UUID, T_CORR), xsd("schema_payment_registered_v2.1.xsd"), "payment_registered", "kassa", receiver_fns=rec)
        except Exception as e: fail(f"Kassa setup error: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# Planning Suite
# ═══════════════════════════════════════════════════════════════════════════════

def test_planning(args):
    header("Planning Audit")
    repos = Path(args.repos_dir); p_dir = find_repo(repos, "Planning")
    if not p_dir: fail("Planning repo not found"); return
    sys.path.insert(0, str(p_dir))
    _stub_module("log_publisher", publish_log=MagicMock(), action_for_type=lambda t:t)
    _stub_module("db_config", get_database_connection=lambda:MagicMock())
    _stub_module("graph_service", GraphService=MagicMock())
    try:
        for m in ["producer","consumer"]:
            if m in sys.modules: del sys.modules[m]
        import producer as prod; import consumer as cons
        xsd = lambda n: p_dir / "xsd" / n
        def p_rec(b):
            ch = MagicMock()
            with patch("pika.BlockingConnection"): cons.on_message(ch, MagicMock(routing_key='test'), MagicMock(), b)
            return ch.basic_ack.called, "Nacked"
        rec = {"planning": p_rec}
        _run_case(args, "planning/session_created", lambda: prod.create_session_xml(T_SESSION, "T", T_NOW, T_NOW, "A", 100, 0), xsd("session_created.xsd"), "session_created", "planning", receiver_fns=rec)
        _run_case(args, "planning/session_updated", lambda: prod.create_session_updated_xml(T_SESSION, "U", T_NOW, T_NOW, "B", max_attendees=200, current_attendees=10), xsd("session_updated.xsd"), "session_updated", "planning", receiver_fns=rec)
    except Exception as e: fail(f"Planning setup error: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# Facturatie Suite
# ═══════════════════════════════════════════════════════════════════════════════

def test_facturatie(args):
    header("Facturatie Audit")
    repos = Path(args.repos_dir); f_dir = find_repo(repos, "Facturatie")
    if not f_dir: fail("Facturatie repo not found"); return
    sys.path.insert(0, str(f_dir))
    import importlib as _il
    for k in ["src","src.services","src.utils", "defusedxml", "mysql", "mysql.connector"]: 
        if k in sys.modules: del sys.modules[k]
    try:
        _stub_module("defusedxml.ElementTree", fromstring=etree.fromstring)
        _stub_module("mysql.connector", connect=MagicMock()); _stub_module("mysql.connector.pooling", MySQLConnectionPool=MagicMock())
        _il.import_module("src"); _il.import_module("src.services")
        _stub_module("src.services.rabbitmq_utils", get_connection=MagicMock(), get_connection_with_retry=MagicMock(), send_to_dlq=MagicMock())
        _stub_module("src.utils.xml_validator", validate_xml=lambda x,s=None:(True,None))
        _stub_module("src.services.fossbilling_api", create_registration_invoice=lambda *a: "INV-1", pay_invoice=lambda *a: True)
        _stub_module("src.services.identity_client", request_master_uuid=lambda *a: T_UUID)
        _stub_module("src.services.consumption_store", store_consumption=MagicMock())
        
        s = _il.import_module("src.services.rabbitmq_sender")
        r = _il.import_module("src.services.rabbitmq_receiver")
        xsd = lambda n: f_dir / "src" / "services" / "xsd" / n
        def f_rec(b):
            ch = MagicMock()
            r.process_message(ch, MagicMock(delivery_tag=1), MagicMock(), b)
            return ch.basic_ack.called, "Nacked"
        rec = {"facturatie": f_rec}
        # Fixed arguments for Facturatie builder: invoice_id, email, corr_id, first, last, customer_id, identity_uuid
        _run_case(args, "facturatie/send_mailing", lambda: s.build_invoice_created_notification_xml("I1","t@e.com",T_CORR,"J","J","",T_UUID), xsd("send_mailing.xsd"), "send_mailing", "facturatie", receiver_fns=rec)
        _run_case(args, "facturatie/payment_confirmed", lambda: s.build_payment_confirmed_xml("I1", T_UUID, "75.00", "eur", "online", T_NOW, "paid", T_DATE, "T1"), xsd("payment_registered.xsd"), "payment_registered", "facturatie", receiver_fns=rec)
    except Exception as e: fail(f"Facturatie setup error: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# CRM Suite (Node.js)
# ═══════════════════════════════════════════════════════════════════════════════

def test_crm(args):
    header("CRM Audit (Salesforce Integration)")
    repos = Path(args.repos_dir); c_dir = find_repo(repos, "CRM")
    if not c_dir: fail("CRM repo not found"); return
    def run_node(mode, m, d):
        script = f"""
        const Module = require('module'); const orig = Module.prototype.require;
        const mockSF = {{
            init: async () => true, isConnected: true,
            apiCall: async (fn) => {{
                const conn = {{ sobject: (type) => ({{
                    upsert: async (data) => {{
                        if (type === 'Member__c' && data.User_ID__c !== undefined) throw new Error("INVALID_FIELD: No such column 'User_ID__c' on Member__c");
                        if (type === 'Consumption__c' && data.VAT_Rate__c !== undefined) throw new Error("INVALID_FIELD: No such column 'VAT_Rate__c' on Consumption__c");
                        return {{ success: true, id: 'SF-ID' }};
                    }},
                    create: async () => ({{ success: true, id: 'SF-ID' }}), update: async () => ({{ success: true, id: 'SF-ID' }}),
                    find: () => ({{ limit: () => ({{ execute: async () => [] }}) }})
                }}) }}; return await fn(conn);
            }}
        }};
        const mockMQ = {{
            assertQueue: async () => {{}}, assertExchange: async () => {{}}, bindQueue: async () => {{}},
            consume: async () => ({{ consumerTag: 'tag' }}), sendToQueue: () => true, publish: () => true, ack: () => {{}}, nack: () => {{}}
        }};
        Module.prototype.require = function(p) {{
            if (p === 'libxmljs2') return {{ parseXml: () => ({{ validate: () => true }}) }};
            if (p === 'amqplib') return {{ connect: async () => ({{ createChannel: async () => mockMQ }}) }};
            if (p.includes('sfConnection')) return function() {{ return mockSF; }};
            if (p.includes('sender')) return function() {{ 
                function Mock() {{}}
                Mock.prototype.init = async () => {{}}; Mock.prototype.sendLog = async () => {{}};
                const realClass = orig.apply(this, arguments); if (typeof realClass === 'function') Object.assign(Mock.prototype, realClass.prototype);
                return Mock;
            }};
            return orig.apply(this, arguments);
        }};
        async function run() {{
            try {{
                if ("{mode}" === "build") {{
                    const S = require('./src/sender'); const s = new S();
                    const res = await s.{m}({json.dumps(d)}); process.stdout.write(String(res));
                }} else {{
                    const R = require('./src/receiver'); const r = new R();
                    r.channel = mockMQ; r.sf = mockSF; r.sender = {{ init: async()=>{{}}, sendLog: async()=>{{}}, sendNewRegistrationToKassa: async()=>({{}}), sendConsumptionOrderToFacturatie: async()=>({{}}) }};
                    const msg = {{ content: Buffer.from({json.dumps(d)}), fields: {{ deliveryTag: 1 }}, properties: {{ contentType: 'application/xml' }} }};
                    let logs = []; console.log = (...a) => logs.push(a.join(' ')); console.error = (...a) => logs.push(a.join(' '));
                    await r.handleMessage(msg);
                    const err = logs.find(l => (l.includes('Error') || l.includes('INVALID_FIELD')) && !l.includes('WARNING'));
                    if (err) {{ process.stderr.write(err); process.exit(1); }}
                    process.stdout.write("SUCCESS");
                }}
            }} catch (e) {{ process.stderr.write(e.message); process.exit(1); }}
        }}
        run();
        """
        res = subprocess.run(["node", "-e", script], cwd=str(c_dir), capture_output=True, text=True)
        return res.returncode == 0, res.stdout if res.returncode == 0 else res.stderr
    xsd = lambda n: c_dir / "xsd" / n
    def c_rec(b): return run_node("process", "handleMessage", b.decode('utf-8'))
    reg_data = {"customer":{"identity_uuid":T_UUID,"email":"lena.declercq@test.be","first_name":"Lena","last_name":"Declercq","type":"private"},"session_id":T_SESSION,"payment_due":{"amount":"10.00","status":"unpaid"},"correlation_id":T_CORR}
    _run_case(args, "crm/new_registration", lambda: run_node("build", "buildNewRegistrationForKassaXml", reg_data)[1], xsd("new_registration_kassa.xsd"), "new_registration", "crm", receiver_fns={"crm": c_rec})
    cons_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<message><header><message_id>{T_CORR}</message_id><timestamp>{T_NOW}</timestamp><source>kassa</source><type>consumption_order</type><version>2.0</version></header>
<body><is_anonymous>false</is_anonymous><customer><id>42</id><identity_uuid>{T_UUID}</identity_uuid><type>private</type></customer>
<items><item><id>1</id><sku>S1</sku><description>Beer</description><quantity>1</quantity><unit_price currency="eur">3.00</unit_price><vat_rate>21</vat_rate><total_amount currency="eur">3.00</total_amount></item></items></body></message>"""
    _run_case(args, "crm/consumption_order", lambda: cons_xml, xsd("consumption_order.xsd"), "consumption_order", "kassa", receiver_fns={"crm": c_rec})

# ═══════════════════════════════════════════════════════════════════════════════
# Heartbeat, Identity, Mailing, Monitoring
# ═══════════════════════════════════════════════════════════════════════════════

def test_heartbeat(args):
    header("Heartbeat Audit")
    repos = Path(args.repos_dir); h_dir = find_repo(repos, "heartbeat")
    if not h_dir: fail("Heartbeat repo not found"); return
    with patch.dict(os.environ, {"SYSTEM_NAME":"test","TARGETS":"127.0.0.1:80","RABBITMQ_HOST":"127.0.0.1","RABBITMQ_USER":"g","RABBITMQ_PASS":"g"}):
        with patch("pika.BlockingConnection"), patch("socket.getaddrinfo", return_value=[(0,0,0,"",("127.0.0.1",80))]), patch("time.sleep", side_effect=InterruptedError("loop")):
            sys.path.insert(0, str(h_dir))
            if "sidecar" in sys.modules: del sys.modules["sidecar"]
            _stub_module("lxml.etree", XMLSchema=MagicMock())
            try: import sidecar
            except (InterruptedError, Exception): pass
            hb_mod = sys.modules.get("sidecar")
            if hb_mod: _run_case(args, "heartbeat/heartbeat", lambda: hb_mod.build_heartbeat_xml("hb_service", "online", 3600), h_dir / "heartbeat.xsd", "heartbeat", "hb_service")

def test_identity(args):
    header("Identity Audit")
    repos = Path(args.repos_dir); i_dir = find_repo(repos, "identity-service")
    if not i_dir: fail("Identity repo not found"); return
    sys.path.insert(0, str(i_dir))
    _stub_module("database", SessionLocal=MagicMock()); _stub_module("defusedxml.ElementTree", fromstring=etree.fromstring)
    try:
        if "rabbitmq_service" in sys.modules: del sys.modules["rabbitmq_service"]
        import rabbitmq_service as i_svc
        def capture_pub():
            captured = []
            with patch("rabbitmq_service.get_rabbitmq_connection") as mock_conn:
                mock_ch = mock_conn.return_value.channel.return_value
                mock_ch.basic_publish.side_effect = lambda *a,**k: captured.append(k.get("body",b"").decode("utf-8"))
                i_svc.publish_user_created(T_UUID, "t@e.com", "id-service")
            return captured[0] if captured else None
        _run_case(args, "identity/user_created", capture_pub, repos / "contracts" / "xsd" / "identity_event.xsd", "UserCreated", "id-service", flat_root="user_event")
    except Exception as e: fail(f"Identity setup error: {e}")

def test_mailing(args):
    header("Mailing Audit")
    repos = Path(args.repos_dir); m_dir = find_repo(repos, "Mailing")
    if not m_dir: fail("Mailing repo not found"); return
    ms = m_dir / "mailing_service"
    sys.path.insert(0, str(ms))
    _stub_module("sendgrid_client"); _stub_module("envelope")
    try:
        from publishers import mailing_status, logs
        xsd = lambda n: ms / "schemas" / n
        _run_case(args, "mailing/mailing_status", lambda: etree.tostring(mailing_status._build_element(correlation_id=T_CORR, campaign_id="C1", subject="S", sent=1, delivered=1, bounced=0, opened=0, bounced_emails=[], status="completed"), encoding="unicode"), xsd("mailing_status.xsd"), "mailing_status", "mailing")
    except Exception as e: fail(f"Mailing setup error: {e}")

def test_monitoring(args):
    header("Monitoring Audit")
    repos = Path(args.repos_dir); mon_dir = find_repo(repos, "monitoring")
    if not mon_dir: fail("Monitoring repo not found"); return
    det = mon_dir / "detector" / "detector.py"
    sys.path.insert(0, str(det.parent))
    _stub_module("elasticsearch", Elasticsearch=MagicMock())
    with patch.dict(os.environ, {"RABBITMQ_HOST":"l","RABBITMQ_PORT":"5672","RABBITMQMONITORING_USER":"g","RABBITMQMONITORING_PASS":"g","RABBITMQ_VHOST":"/"}):
        with patch("pika.BlockingConnection") as mock_conn:
            try:
                if "detector" in sys.modules: del sys.modules["detector"]
                _stub_module("logging").getLogger = MagicMock()
                import detector
                def capture_alert():
                    mock_ch = mock_conn.return_value.channel.return_value
                    captured = []
                    mock_ch.basic_publish.side_effect = lambda *a,**k: captured.append(k.get("body",""))
                    detector.send_alert_xml("kassa"); return captured[0] if captured else None
                _run_case(args, "monitoring/system_alert", capture_alert, mon_dir / "xsd" / "system_alert.xsd", "HEARTBEAT_CRITICAL", "monitoring", flat_root="alert")
            except Exception as e: fail(f"Monitoring setup error: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    args = parse_args(); load_env(args.env)
    t = args.teams.lower(); all_t = t == "all"; t_run = []
    print(f"\n{BOLD}BEHAVIORAL INTEGRATION AUDIT — v2.3{RESET}")
    print(f"Goal: Executing team code to verify generation and processing logic.")
    
    if all_t or "kassa" in t: test_kassa(args); t_run.append("kassa")
    if all_t or "planning" in t: test_planning(args); t_run.append("planning")
    if all_t or "facturatie" in t: test_facturatie(args); t_run.append("facturatie")
    if all_t or "heartbeat" in t: test_heartbeat(args); t_run.append("heartbeat")
    if all_t or "identity" in t: test_identity(args); t_run.append("identity")
    if all_t or "mailing" in t: test_mailing(args); t_run.append("mailing")
    if all_t or "monitoring" in t: test_monitoring(args); t_run.append("monitoring")
    if all_t or "crm" in t: test_crm(args); t_run.append("crm")
    
    tot, fail_count = _state["tests"], _state["failures"]
    print(f"\n{'═'*60}\n{tot-fail_count} PASSED / {fail_count} FAILED")
    
    sum_file = os.getenv("GITHUB_STEP_SUMMARY")
    if sum_file:
        with open(sum_file, "a", encoding="utf-8") as f:
            f.write(f"## 🧪 Behavioral Audit — `test_contract_full.py` \n\n")
            f.write(f"| Test Case | Phase 1 (XSD) | Phase 3 (Logic) | \n|---|---|---| \n")
            for r in _state["results"]:
                p1 = "✅" if r["p1"]=="pass" else "❌"
                p3 = "✅" if r["p3"]=="pass" else "❌" if r["p3"]=="fail" else "⏭️"
                f.write(f"| `{r['name']}` | {p1} | {p3} | \n")
            failed = [r for r in _state["results"] if r["p1"]=="fail" or r["p3"]=="fail"]
            if failed:
                f.write("\n### ⚠️ Behavioral Failures \n\n")
                for r in failed:
                    if r["p1"] == "fail": f.write(f"**`{r['name']} (XSD)`** — {r['error']} \n\n")
                    if r["p3"] == "fail": f.write(f"**`{r['name']} (Logic)`** — {r['proc_error']} \n\n")
    sys.exit(1 if fail_count else 0)

if __name__ == "__main__": main()
