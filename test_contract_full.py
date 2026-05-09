"""
test_contract_full.py  —  High-fidelity builder compliance audit
Groep 1 — Desideriushogeschool 2026  /  XML/XSD Contract v2.3

This harness executes the ACTUAL builder functions from team repositories and
validates their output against the centralized contract XSDs. No XML templates
are hardcoded here; failures represent genuine deviations in production code.
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
    import pika
    _PIKA_AVAILABLE = True
except ImportError:
    _PIKA_AVAILABLE = False

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

# ── Stable Test Data ───────────────────────────────────────────────────────────
T_UUID    = "11111111-2222-3333-4444-555555555555"
T_CORR    = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
T_SESSION = "cccccccc-dddd-eeee-ffff-000000000001"
T_BADGE   = "BADGE-RF-00142"
T_DATE    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
T_NOW     = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ── Dynamic Import & Stubbing ──────────────────────────────────────────────────
def _stub_module(name: str, **attrs):
    parts = name.split(".")
    for i in range(1, len(parts) + 1):
        n = ".".join(parts[:i])
        if n not in sys.modules:
            sys.modules[n] = types.ModuleType(n)
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
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--env", default=".env")
    return p.parse_args()

# ═══════════════════════════════════════════════════════════════════════════════
# Validation Engine
# ═══════════════════════════════════════════════════════════════════════════════

def validate_against_xsd(xml_str: str, xsd_path: Path) -> tuple[bool, str | None]:
    if not _LXML_AVAILABLE: return False, "lxml missing"
    if not xsd_path.exists(): return False, f"XSD missing: {xsd_path}"
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
        else:
            try: uuid.UUID((mid.text or "").strip())
            except: issues.append(f"Invalid UUID in message_id: {mid.text}")
        
        if src is None: issues.append("Missing <header/source>")
        elif (src.text or "").strip() != expected_source: issues.append(f"Source mismatch: got {src.text}, want {expected_source}")
        if typ is None: issues.append("Missing <header/type>")
        elif (typ.text or "").strip() != expected_type: issues.append(f"Type mismatch: got {typ.text}, want {expected_type}")
        if ver is None: issues.append("Missing <header/version>")
        elif (ver.text or "").strip() != "2.0": issues.append(f"Version mismatch: got {ver.text}, want 2.0")
    elif root.tag != flat_root:
        issues.append(f"Root mismatch: got <{root.tag}>, want <{flat_root}>")
    return issues

def _run_case(args, name, builder_fn, xsd_path, msg_type, source, flat_root=None):
    print(f"\n  [{name}]")
    _state["tests"] += 1
    res = {"name": name, "p1": "fail", "error": ""}
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
            ok("Structural pass")
            valid, err = validate_against_xsd(xml_str, xsd_path)
            if valid:
                ok(f"XSD valid ({xsd_path.name})")
                res["p1"] = "pass"
            else:
                fail(f"XSD invalid ({xsd_path.name}): {err}")
                res["error"] = err
    except Exception as e:
        fail(f"Execution error: {e}")
        res["error"] = str(e)
    
    if res["p1"] == "fail": _state["failures"] += 1
    _state["results"].append(res)

# ═══════════════════════════════════════════════════════════════════════════════
# Team Audits
# ═══════════════════════════════════════════════════════════════════════════════

def test_kassa(args):
    header("Kassa")
    repos = Path(args.repos_dir); k_dir = repos / "Kassa" / "integratie"
    if not k_dir.exists(): fail("Kassa repo not found"); return
    with patch.dict(os.environ, {"RABBIT_HOST":"localhost","RABBIT_USER":"guest","RABBIT_PASS":"guest"}):
        sys.path.insert(0, str(k_dir))
        try:
            if "sender" in sys.modules: del sys.modules["sender"]
            import sender as k_sender
            xsd = lambda n: k_dir / "schemas" / n
            
            _run_case(args, "kassa/consumption_order", lambda: k_sender.build_consumption_order_xml([{"id":"1","sku":"S1","description":"T","quantity":1,"unit_price":"5.0","vat_rate":"21","total_amount":5.0,"currency":"eur","item_type":"food"}], "42", T_UUID, "private", "t@e.com", {"street":"S","number":"1","postal_code":"1","city":"B","country":"be"}), xsd("schema_consumption_order_v2.3.xsd"), "consumption_order", "kassa")
            _run_case(args, "kassa/payment_registered", lambda: k_sender.build_payment_registered_xml("consumption", "paid", "10.0", T_DATE, "T1", "on_site", "I1", T_UUID, T_CORR), xsd("schema_payment_registered_v2.1.xsd"), "payment_registered", "kassa")
            _run_case(args, "kassa/invoice_request", lambda: k_sender.build_invoice_request_xml(T_UUID, {"first_name":"J","last_name":"J","email":"t@e.com","address":{"street":"S","number":"1","postal_code":"1","city":"B","country":"be"}}, T_CORR), xsd("schema_invoice_request.xsd"), "invoice_request", "kassa")
            _run_case(args, "kassa/badge_assigned", lambda: k_sender.build_badge_assigned_xml(T_BADGE, T_UUID), xsd("schema_badge_assigned.xsd"), "badge_assigned", "kassa")
            _run_case(args, "kassa/wallet_balance_update", lambda: k_sender.build_wallet_balance_update_xml(T_UUID, 42.50), xsd("schema_wallet_balance_update.xsd"), "wallet_balance_update", "kassa")
            _run_case(args, "kassa/wallet_lease_request", lambda: k_sender.build_wallet_lease_request_xml(T_UUID, T_BADGE), xsd("schema_wallet_lease_request.xsd"), "wallet_lease_request", "kassa")
            _run_case(args, "kassa/wallet_lease_return", lambda: k_sender.build_wallet_lease_return_xml(T_UUID, 15.75, T_CORR, 3), xsd("schema_wallet_lease_return.xsd"), "wallet_lease_return", "kassa")
            _run_case(args, "kassa/refund_processed", lambda: k_sender.build_refund_processed_xml(T_CORR, "consumption_item", 5.00, "badge_wallet", "customer_request", "T1", T_UUID), xsd("schema_refund_processed.xsd"), "refund_processed", "kassa")
            
        except Exception as e: fail(f"Kassa setup error: {e}")

def test_planning(args):
    header("Planning")
    repos = Path(args.repos_dir); p_dir = repos / "Planning"
    if not p_dir.exists(): fail("Planning repo not found"); return
    sys.path.insert(0, str(p_dir))
    _stub_module("log_publisher", publish_log=MagicMock(), action_for_type=lambda t:t)
    try:
        if "producer" in sys.modules: del sys.modules["producer"]
        import producer as p_prod
        xsd = lambda n: p_dir / "xsd" / n
        _run_case(args, "planning/session_created", lambda: p_prod.create_session_xml(T_SESSION, "T", "2026-09-01T10:00:00Z", "2026-09-01T11:00:00Z", "A", 100, 0), xsd("session_created.xsd"), "session_created", "planning")
        _run_case(args, "planning/session_updated", lambda: p_prod.create_session_updated_xml(T_SESSION, "U", "2026-09-01T10:30:00Z", "2026-09-01T11:30:00Z", "B", max_attendees=200, current_attendees=10), xsd("session_updated.xsd"), "session_updated", "planning")
        _run_case(args, "planning/session_deleted", lambda: p_prod.create_session_deleted_xml(T_SESSION, "Cancelled", "admin"), xsd("session_deleted.xsd"), "session_deleted", "planning")
        _run_case(args, "planning/session_view_request", lambda: p_prod.create_session_view_request_xml(T_SESSION), xsd("session_view_request.xsd"), "session_view_request", "frontend")
    except Exception as e: fail(f"Planning setup error: {e}")

def test_facturatie(args):
    header("Facturatie")
    repos = Path(args.repos_dir); f_dir = repos / "Facturatie"
    if not f_dir.exists(): fail("Facturatie repo not found"); return
    sys.path.insert(0, str(f_dir))
    import importlib as _il
    for k in ["src","src.services","src.utils"]: 
        if k in sys.modules: del sys.modules[k]
    try:
        _il.import_module("src"); _il.import_module("src.services")
        _stub_module("src.services.rabbitmq_utils", get_connection=lambda:MagicMock())
        _stub_module("src.utils.xml_validator", validate_xml=lambda x,s=None:(True,None))
        if "src.services.rabbitmq_sender" in sys.modules: del sys.modules["src.services.rabbitmq_sender"]
        f_sender = _il.import_module("src.services.rabbitmq_sender")
        xsd = lambda n: f_dir / "src" / "services" / "xsd" / n
        
        _run_case(args, "facturatie/send_mailing", lambda: f_sender.build_invoice_created_notification_xml("I1","t@e.com",T_CORR,"J","J",T_UUID,"S"), xsd("send_mailing.xsd"), "send_mailing", "facturatie")
        _run_case(args, "facturatie/consumption_order", lambda: f_sender.build_consumption_order_xml(T_UUID, [{"id":1,"description":"T","quantity":1,"unit_price":"5.0","vat_rate":"0"}]), xsd("consumption_order.xsd"), "consumption_order", "kassa")
        _run_case(args, "facturatie/payment_confirmed", lambda: f_sender.build_payment_confirmed_xml("I1", T_UUID, "75.00", "eur", "online", T_NOW, "paid", T_DATE, "T1"), xsd("payment_registered.xsd"), "payment_registered", "facturatie")
        
    except Exception as e: fail(f"Facturatie setup error: {e}")

def test_heartbeat(args):
    header("Heartbeat")
    repos = Path(args.repos_dir); h_file = repos / "heartbeat" / "sidecar.py"
    if not h_file.exists(): fail("Heartbeat sidecar not found"); return
    with patch.dict(os.environ, {"SYSTEM_NAME":"test","TARGETS":"127.0.0.1:80","RABBITMQ_HOST":"127.0.0.1","RABBITMQ_USER":"guest","RABBITMQ_PASS":"guest"}):
        with patch("pika.BlockingConnection"), patch("socket.getaddrinfo", return_value=[(0,0,0,"",("127.0.0.1",80))]), patch("time.sleep", side_effect=InterruptedError("loop")):
            sys.path.insert(0, str(h_file.parent))
            if "sidecar" in sys.modules: del sys.modules["sidecar"]
            _stub_module("lxml.etree", XMLSchema=MagicMock())
            try: import sidecar
            except InterruptedError: pass
            except Exception as e: fail(f"Heartbeat import error: {e}"); return
            hb_mod = sys.modules.get("sidecar")
            _run_case(args, "heartbeat/heartbeat", lambda: hb_mod.build_heartbeat_xml("hb_service", "online", 3600), h_file.parent / "heartbeat.xsd", "heartbeat", "hb_service")

def test_identity(args):
    header("Identity")
    repos = Path(args.repos_dir); i_file = repos / "identity-service" / "rabbitmq_service.py"
    if not i_file.exists(): fail("Identity service not found"); return
    sys.path.insert(0, str(i_file.parent))
    _stub_module("database", SessionLocal=MagicMock())
    _stub_module("defusedxml.ElementTree", fromstring=MagicMock())
    try:
        if "rabbitmq_service" in sys.modules: del sys.modules["rabbitmq_service"]
        import rabbitmq_service as i_svc
        
        def capture_publish():
            captured = []
            with patch("rabbitmq_service.get_rabbitmq_connection") as mock_conn:
                mock_chan = mock_conn.return_value.channel.return_value
                def side_effect(*a, **k): captured.append(k.get("body", b"").decode("utf-8"))
                mock_chan.basic_publish.side_effect = side_effect
                i_svc.publish_user_created(T_UUID, "t@e.com", "id-service")
            return captured[0] if captured else None
        _run_case(args, "identity/user_created", capture_publish, repos / "contracts" / "xsd" / "identity_event.xsd", "UserCreated", "id-service", flat_root="user_event")
        
        _run_case(args, "identity/ok_response", lambda: i_svc._build_ok_response(MagicMock(master_uuid=T_UUID, email="t@e.com", created_by="admin", created_at=datetime.now(timezone.utc))), None, "identity_response", "identity-service", flat_root="identity_response")
        _run_case(args, "identity/error_response", lambda: i_svc._build_error_response("E1", "Err"), None, "identity_response", "identity-service", flat_root="identity_response")
        
    except Exception as e: fail(f"Identity setup error: {e}")

def test_mailing(args):
    header("Mailing")
    repos = Path(args.repos_dir); m_dir = repos / "Mailing" / "mailing_service"
    if not m_dir.exists(): fail("Mailing repo not found"); return
    sys.path.insert(0, str(m_dir))
    _stub_module("sendgrid_client"); _stub_module("envelope")
    try:
        from publishers import mailing_status, logs
        xsd = lambda n: m_dir / "schemas" / n
        _run_case(args, "mailing/mailing_status", lambda: etree.tostring(mailing_status._build_element(correlation_id=T_CORR, campaign_id="C1", subject="S", sent=1, delivered=1, bounced=0, opened=0, bounced_emails=[], status="completed"), encoding="unicode"), xsd("mailing_status.xsd"), "mailing_status", "mailing")
        def b_log():
            import logging
            r = logging.LogRecord("t", logging.INFO, "p", 10, "M", None, None)
            r.action = "email"; return logs._record_to_xml(r)
        _run_case(args, "mailing/log", b_log, xsd("logs.xsd"), "log", "mailing")
    except Exception as e: fail(f"Mailing setup error: {e}")

def test_monitoring(args):
    header("Monitoring")
    repos = Path(args.repos_dir); det_file = repos / "monitoring" / "detector" / "detector.py"
    if not det_file.exists(): fail("Monitoring detector not found"); return
    sys.path.insert(0, str(det_file.parent))
    _stub_module("elasticsearch", Elasticsearch=MagicMock())
    try:
        with patch.dict(os.environ, {"RABBITMQ_HOST":"localhost","RABBITMQ_PORT":"5672","RABBITMQMONITORING_USER":"guest","RABBITMQMONITORING_PASS":"guest","RABBITMQ_VHOST":"/"}):
            with patch("pika.BlockingConnection") as mock_conn:
                if "detector" in sys.modules: del sys.modules["detector"]
                import detector
                def capture_alert():
                    mock_chan = mock_conn.return_value.channel.return_value
                    captured = []
                    def side_effect(*a, **k): captured.append(k.get("body", ""))
                    mock_chan.basic_publish.side_effect = side_effect
                    detector.send_alert_xml("kassa")
                    return captured[0] if captured else None
                _run_case(args, "monitoring/system_alert", capture_alert, repos / "monitoring" / "xsd" / "system_alert.xsd", "HEARTBEAT_CRITICAL", "monitoring", flat_root="alert")
    except Exception as e: fail(f"Monitoring setup error: {e}")

def test_crm(args):
    header("CRM")
    repos = Path(args.repos_dir); c_dir = repos / "CRM"
    if not c_dir.exists(): fail("CRM repo not found"); return
    def run_node(m, d):
        script = f"const Module = require('module'); const orig = Module.prototype.require; Module.prototype.require = function(p) {{ if (p === 'libxmljs2') return {{ parseXml: () => ({{ validate: () => true }}) }}; if (p === 'amqplib') return {{ connect: () => ({{ createChannel: () => ({{ assertQueue: () => {{}} }}) }}) }}; return orig.apply(this, arguments); }}; try {{ const S = require('./src/sender'); const s = new S(); process.stdout.write(s.{m}({json.dumps(d)})); }} catch (e) {{ process.stderr.write(e.message); process.exit(1); }}"
        res = subprocess.run(["node", "-e", script], cwd=str(c_dir), capture_output=True, text=True)
        if res.returncode != 0: raise RuntimeError(f"Node.js error: {res.stderr}")
        return res.stdout
    xsd = lambda n: c_dir / "xsd" / n
    _run_case(args, "crm/new_registration", lambda: run_node("buildNewRegistrationForKassaXml", {"customer":{"identity_uuid":T_UUID,"email":"t@e.com","first_name":"T","last_name":"T","type":"private"},"session_id":T_SESSION,"payment_due":"10.0","correlation_id":T_CORR}), xsd("new_registration_kassa.xsd"), "new_registration", "crm")
    _run_case(args, "crm/profile_update", lambda: run_node("buildProfileUpdateXml", {"identity_uuid":T_UUID,"email":"t@e.com","first_name":"U","last_name":"N","correlation_id":T_CORR}), xsd("profile_update.xsd"), "profile_update", "crm")
    _run_case(args, "crm/cancel_registration", lambda: run_node("buildCancelRegistrationXml", {"identity_uuid":T_UUID,"session_id":T_SESSION,"correlation_id":T_CORR}), xsd("cancel_registration.xsd"), "cancel_registration", "crm")
    _run_case(args, "crm/invoice_request", lambda: run_node("buildInvoiceRequestXml", {"identity_uuid":T_UUID, "correlation_id":T_CORR, "customer":{"email":"t@e.com","address":{"country":"be"}}}), xsd("invoice_request_facturatie.xsd"), "invoice_request", "crm")
    _run_case(args, "crm/mailing_send", lambda: run_node("buildMailingSendXml", {"campaign_id":"C1","subject":"S","mail_type":"T","recipients":[{"email":"t@e.com","identity_uuid":T_UUID,"contact":{"first_name":"J","last_name":"J"}}],"correlation_id":T_CORR}), xsd("send_mailing.xsd"), "send_mailing", "crm")
    _run_case(args, "crm/user_unregistered", lambda: run_node("buildUserUnregisteredXml", {"identity_uuid":T_UUID, "correlation_id":T_CORR}), None, "user.unregistered", "crm")
    _run_case(args, "crm/log", lambda: run_node("buildLogXml", {"level":"info","action":"user","message":"T"}), xsd("log.xsd"), "log", "crm")

def main():
    args = parse_args(); load_env(args.env)
    t = args.teams.lower(); all_t = t == "all"; t_run = []
    print(f"\n{BOLD}Contract Compliance Audit — v2.3{RESET}")
    print(f"Repos Dir: {args.repos_dir}")
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
            f.write(f"## 🧪 Builder Compliance — `test_contract_full.py`\\n\\n")
            f.write(f"| | |\\n|---|---|\\n| **Result** | {'✅ PASS' if fail_count==0 else '❌ FAIL'} |\\n| **Passed** | {tot-fail_count} / {tot} |\\n| **Failed** | {fail_count} |\\n| **Teams** | {', '.join(t_run)} |\\n\\n")
            f.write("### Results per test case\\n\\n| Test case | Status |\\n|---|---|\\n")
            for r in _state["results"]: f.write(f"| `{r['name']}` | {'✅ PASS' if r['p1']=='pass' else '❌ FAIL'} |\\n")
            failed = [r for r in _state["results"] if r["p1"]=="fail"]
            if failed:
                f.write("\\n### ⚠️ Failures\\n\\n")
                for r in failed: f.write(f"**`{r['name']}`** — {r['error']}\\n\\n")
    sys.exit(1 if fail_count else 0)

if __name__ == "__main__": main()
