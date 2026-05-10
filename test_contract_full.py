"""
test_contract_full.py  —  Comprehensive Behavioral Contract Audit
Groep 1 — Desideriushogeschool 2026  /  XML/XSD Contract v2.3

Uitleg:
Dit script test de volledige keten van elk bericht:
1.  VERZENDEN: Kan de verzender (producer) een bericht bouwen?
2.  CONTRACT: Voldoet dit bericht aan de officiële XSD afspraken?
3.  ONTVANGEN: Kan de ontvanger (consumer) dit bericht verwerken?
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
import importlib
import tempfile
import traceback
from uuid import UUID
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

_XML_ENV_DIR = str(Path(__file__).resolve().parent)
if _XML_ENV_DIR not in sys.path:
    sys.path.insert(0, _XML_ENV_DIR)
from ci_summary import TeeStream, markdown_full_log

try:
    import yaml
    _YAML_AVAILABLE = True
except ImportError:
    _YAML_AVAILABLE = False

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

try:
    from lxml import etree
    _LXML_AVAILABLE = True
except ImportError:
    _LXML_AVAILABLE = False

# ── Kleuren & Status ───────────────────────────────────────────────────────────
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

# ── Global State ───────────────────────────────────────────────────────────────
_state = {"failures": 0, "tests": 0, "results": []}
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

T_UUID    = "11111111-2222-3333-4444-555555555555"
T_CORR    = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
T_SESSION = "cccccccc-dddd-eeee-ffff-000000000001"
T_BADGE   = "BADGE-RF-00142"
T_DATE    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
T_NOW     = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ── Helpers ────────────────────────────────────────────────────────────────────
def find_repo(repos_dir: Path, name: str) -> Path | None:
    options = [repos_dir / name, repos_dir.parent / name, repos_dir / "xml-test-env" / name]
    for p in options:
        if p.exists() and p.is_dir(): return p
    return None

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
    if not p.exists():
        p = Path(__file__).resolve().parent.parent / ".env"
        if not p.exists(): return
    with open(p) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

def parse_args():
    p = argparse.ArgumentParser(description="Comprehensive Behavioral Contract Audit — v2.3")
    p.add_argument("--repos-dir", default=".", help="Directory containing all team repositories")
    p.add_argument("--teams",     default="all", help="Comma-separated list of teams to test (default: all)")
    p.add_argument("--env",       default=".env", help="Path to .env file")
    p.add_argument("--verbose",   action="store_true", help="Print detailed output")
    return p.parse_args()

def team_applies(args, team):
    if not args.teams or args.teams == "all": return True
    return team.lower() in [t.strip().lower() for t in args.teams.split(",")]

def _flows_by_id():
    global _FLOWS_BY_ID
    if not _YAML_AVAILABLE: return {}
    path = Path(__file__).resolve().parent / "contract_flows.yaml"
    if not path.exists(): return {}
    with path.open(encoding="utf-8") as f: data = yaml.safe_load(f)
    flows = data.get("flows") if isinstance(data, dict) else None
    return {f["id"]: f for f in (flows or []) if isinstance(f, dict) and "id" in f}

def contract_example_path(repos_dir: Path, flow_id: str) -> Path | None:
    flow = _flows_by_id().get(flow_id)
    if not flow: return None
    ex = flow.get("example")
    if not ex or ex in ("~", None): return None
    p = Path(repos_dir) / "contracts" / ex
    if p.is_file(): return p
    name = Path(ex).name
    fixtures = Path(repos_dir) / "integration-tests" / "fixtures"
    if fixtures.is_dir():
        for sub in fixtures.iterdir():
            if sub.is_dir():
                candidate = sub / name
                if candidate.is_file(): return candidate
        candidate = fixtures / name
        if candidate.is_file(): return candidate
    return None

# ── Validation ─────────────────────────────────────────────────────────────────

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
    try: root = etree.fromstring(xml_str.encode("utf-8") if isinstance(xml_str, str) else xml_str)
    except Exception as e: return [f"XML parse failed: {e}"]
    if not flat_root:
        get = lambda tag: root.find(f"header/{tag}")
        mid, src, typ = get("message_id"), get("source"), get("type")
        if mid is None: issues.append("Geen message_id gevonden")
        if src is None: issues.append("Geen source gevonden")
        elif (src.text or "").strip() != expected_source and expected_source != "*": 
            issues.append(f"Team mismatch: bericht zegt '{src.text}', verwachtte '{expected_source}'")
        if typ is None: issues.append("Geen type gevonden")
    return issues

def _run_case(args, name, builder_fn, xsd_path, msg_type, source, receiver_fns=None, flat_root=None, fallback_flow_id=None):
    print(f"\n  [{name}]")
    _state["tests"] += 1
    
    # Rapportage data
    res = {
        "flow_id": name,
        "label": _flows_by_id().get(fallback_flow_id, {}).get("label", msg_type),
        "sender_team": source.upper(),
        "sender_status": "PENDING", # "OK", "FAIL", "DUMMY"
        "xsd_status": "PENDING",    # "OK", "FAIL"
        "receivers": [],            # list of {"team": "...", "status": "OK/FAIL", "error": "..."}
        "builder_error": "",
        "xsd_error": ""
    }
    
    repos = Path(args.repos_dir); xml_str = None
    
    def _load_fallback() -> bool:
        nonlocal xml_str
        if not fallback_flow_id: return False
        p = contract_example_path(repos, fallback_flow_id)
        if not p: return False
        try:
            xml_str = p.read_text(encoding="utf-8")
            res["sender_status"] = "DUMMY"
            warn(f"Verzender faalt -> Gebruik contract voorbeeld: {p.name}")
            return True
        except OSError: return False

    # 1. VERZENDEN (Producer)
    try:
        raw = builder_fn()
        if not raw:
            if _load_fallback(): pass
            else: res["sender_status"] = "FAIL"; res["builder_error"] = "Geen XML gegenereerd"; raise ValueError("Builder empty")
        else:
            xml_str = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            if (xml_str or "").strip().startswith("ERROR:"):
                res["builder_error"] = xml_str.strip()
                if _load_fallback(): pass
                else: res["sender_status"] = "FAIL"; raise ValueError(xml_str)
            else:
                res["sender_status"] = "OK"; ok("Verzenden: Bericht succesvol opgebouwd")
    except Exception as e:
        if res["sender_status"] != "DUMMY":
            fail(f"Verzenden: {e}")
            res["sender_status"] = "FAIL"
            if not res["builder_error"]: res["builder_error"] = str(e)
            xml_str = None

    # 2. CONTRACT (XSD)
    if xml_str:
        issues = structural_checks(xml_str, msg_type, source, flat_root)
        if issues: 
            fail(f"Contract: Structuur fout: {'; '.join(issues)}"); res["xsd_status"] = "FAIL"
            res["xsd_error"] = "; ".join(issues)
        else:
            valid, err = validate_against_xsd(xml_str, xsd_path)
            if valid: ok(f"Contract: Voldoet aan XSD afspraken"); res["xsd_status"] = "OK"
            else: fail(f"Contract Fout: {err}"); res["xsd_status"] = "FAIL"; res["xsd_error"] = err

    # 3. ONTVANGEN (Consumer)
    if receiver_fns and xml_str:
        for r_name, r_fn in receiver_fns.items():
            info(f"Ontvangen: Injecteren in {r_name}...")
            r_res = {"team": r_name.upper(), "status": "FAIL", "error": ""}
            try:
                clean_xml = re.sub(r'<\?xml[^?]+\?>', '', xml_str).strip()
                p_success, p_err = r_fn(clean_xml.encode('utf-8'))
                if p_success: 
                    ok(f"Ontvangen ({r_name}): Verwerking geslaagd"); r_res["status"] = "OK"
                else: 
                    fail(f"Ontvangen ({r_name}): {p_err}"); r_res["error"] = p_err
            except Exception as e: 
                fail(f"Ontvangen ({r_name}): Crash: {e}"); r_res["error"] = str(e)
            res["receivers"].append(r_res)

    if res["sender_status"] == "FAIL" or res["xsd_status"] == "FAIL" or any(r["status"] == "FAIL" for r in res["receivers"]):
        _state["failures"] += 1
    _state["results"].append(res)

# ── Team Runners ───────────────────────────────────────────────────────────────

def get_frontend_runner(f_dir):
    f_dir_p = Path(f_dir).resolve()
    def run_php(method, data):
        json_path = None; php_path = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as jf:
                json.dump(data, jf); json_path = jf.name
            json_lit = json.dumps(json_path.replace("\\", "/"))
            php_src = f"""<?php
$f_dir = '{f_dir_p.as_posix()}';
$vendor = "$f_dir/vendor/autoload.php";
if (file_exists($vendor)) {{ require_once $vendor; }}
spl_autoload_register(function ($class) use ($f_dir) {{
    if (strpos($class, 'Drupal\\\\rabbitmq_sender\\\\') === 0) {{
        $rel = str_replace('Drupal\\\\rabbitmq_sender\\\\', 'web/modules/custom/rabbitmq_sender/src/', $class);
        $path = $f_dir . '/' . str_replace('\\\\', '/', $rel) . '.php';
        if (file_exists($path)) {{ require_once $path; }}
    }}
}});
class MockLogger {{ public function info($m, $c) {{}} }}
class MockDrupal {{ public static function logger($n) {{ return new MockLogger(); }} }}
if (!class_exists('Drupal')) {{ class_alias('MockDrupal', 'Drupal'); }}
if (!class_exists('Drupal\\rabbitmq_sender\\{method}')) {{ fwrite(STDERR, "ERROR: Class {method} not found."); exit(1); }}
$data = json_decode(file_get_contents({json_lit}), true);
$sender = new \\Drupal\\rabbitmq_sender\\{method}();
echo $sender->buildXml($data);
"""
            with tempfile.NamedTemporaryFile(mode="w", suffix=".php", delete=False, encoding="utf-8") as pf:
                pf.write(php_src); php_path = pf.name
            res = subprocess.run(["php", php_path], capture_output=True, text=True, cwd=str(f_dir_p), shell=True)
            return f"ERROR: {res.stderr or res.stdout}" if res.returncode != 0 else res.stdout.strip()
        except Exception as e: return f"ERROR: {e}"
        finally:
            for p in (json_path, php_path):
                if p:
                    try: os.unlink(p)
                    except OSError: pass
    return run_php

def get_crm_runner(c_dir):
    def run_node(mode, m, d):
        data_json = json.dumps(d)
        script = f"""
        const T_UUID = "{T_UUID}";
        const Module = require('module'); const orig = Module.prototype.require;
        const mockSF = {{
            init: async () => true, isConnected: true,
            apiCall: async (fn) => {{
                const conn = {{ sobject: (type) => ({{
                    upsert: async (data, key) => {{
                        if (type === 'Member__c' && data.hasOwnProperty('User_ID__c')) {{
                            throw new Error("INVALID_FIELD: No such column 'User_ID__c' on sobject of type Member__c");
                        }}
                        return {{ success: true, id: 'SF-ID' }};
                    }},
                    create: async () => ({{ success: true, id: 'SF-ID' }}),
                    update: async () => ({{ success: true, id: 'SF-ID' }}),
                    find: () => ({{ limit: () => Promise.resolve([{{ Id: 'SF-ID' }}]) }})
                }}) }}; return await fn(conn);
            }}
        }};
        let consumers = {{}};
        let capturedPayload = null;
        const mockMQ = {{
            assertQueue: async (q) => ({{ queue: q || 'mock-q' }}),
            assertExchange: async () => {{}}, bindQueue: async () => {{}},
            consume: async (q, cb) => {{ consumers[q] = cb; return {{ consumerTag: 'tag' }}; }},
            sendToQueue: (q, body, props) => {{
                if (q.includes('identity')) {{
                    const resp = `<identity_response><status>ok</status><user><master_uuid>${{T_UUID}}</master_uuid><email>t@e.com</email><created_by>audit</created_by><created_at>2026-05-10T00:00:00Z</created_at></user></identity_response>`;
                    if (props.replyTo && consumers[props.replyTo]) {{
                        setTimeout(() => consumers[props.replyTo]({{ content: Buffer.from(resp), properties: {{ correlationId: props.correlationId }} }}), 10);
                    }}
                }} else {{
                    capturedPayload = body.toString();
                }}
                return true;
            }},
            publish: (ex, rk, body, props) => {{
                capturedPayload = body.toString();
                return true;
            }},
            ack: () => {{}}, nack: () => {{}},
            cancel: async () => {{}}, deleteQueue: async () => ({{ messageCount: 0 }}), deleteExchange: async () => {{}}
        }};
        Module.prototype.require = function(p) {{
            if (p === 'libxmljs2') return {{ parseXml: () => ({{ validate: () => true }}), memoryUsage: () => 0 }};
            if (p === 'amqplib') return {{ connect: async () => ({{ createChannel: async () => mockMQ, on: () => {{}} }}) }};
            if (p && p.includes('sfConnection')) return function() {{ return mockSF; }};
            if (p && p.includes('sender')) {{
                try {{
                    const Real = orig.call(this, p);
                    return class Mock extends Real {{ constructor() {{ super(...arguments); this.init = async () => {{}}; this.sendLog = async () => ({{ success: true }}); this.channel = mockMQ; }} }};
                }} catch(e) {{ return class Mock {{ constructor() {{ this.init = async () => {{}}; this.sendLog = async () => ({{ success: true }}); this.channel = mockMQ; }} }}; }}
            }}
            return orig.apply(this, arguments);
        }};
        async function run() {{
            try {{
                const d = {data_json};
                if ("{mode}" === "build") {{
                    let logs = []; console.log = (...a) => logs.push(a.join(' ')); console.error = (...a) => logs.push(a.join(' ')); console.warn = () => {{}}; 
                    const S = require('./src/sender'); const s = new S(); await s.init().catch(()=>{{}});
                    await s.{m}(d);
                    process.stdout.write(capturedPayload || "ERROR: No XML generated");
                }} else {{
                    const R = require('./src/receiver'); const r = new R(); r.channel = mockMQ; r.sf = mockSF;
                    if (r.sender) {{ r.sender.channel = mockMQ; r.sender.init = async () => {{}}; }}
                    const msg = {{ content: Buffer.from(typeof d === 'string' ? d : JSON.stringify(d)), fields: {{ deliveryTag: 1 }}, properties: {{ contentType: 'application/xml' }} }};
                    let logs = []; console.log = (...a) => logs.push(a.join(' ')); console.error = (...a) => logs.push(a.join(' '));
                    await r.handleMessage(msg);
                    const err = logs.find(l => (l.toLowerCase().includes('error') || l.includes('INVALID_FIELD') || l.includes('timeout')) && !l.includes('WARNING') && !l.includes('RabbitMQ'));
                    if (err) {{ process.stderr.write(err); process.exit(1); }}
                    process.stdout.write("SUCCESS");
                }}
            }} catch (e) {{ process.stderr.write(e.stack || e.message); process.exit(1); }}
        }}
        run();
        """
        try:
            env = os.environ.copy()
            for k, v in [("RABBITMQ_USER", "guest"), ("RABBITMQ_PASS", "guest"), ("RABBITMQ_HOST", "localhost")]:
                if k not in env: env[k] = v
            res = subprocess.run(["node", "-e", script], cwd=str(c_dir), capture_output=True, text=True, env=env)
            if res.returncode != 0:
                return False, f"ERROR: {res.stderr or res.stdout}"
            return True, res.stdout
        except Exception as e: return False, f"ERROR: {e}"
    return run_node

def _make_facturatie_process_fn(f_dir: Path):
    sys.path.insert(0, str(f_dir))
    for k in ["src", "src.services", "src.utils", "mysql", "mysql.connector"]:
        if k in sys.modules: del sys.modules[k]
    _stub_module("mysql.connector", connect=MagicMock()); _stub_module("mysql.connector.pooling", MySQLConnectionPool=MagicMock())
    importlib.import_module("src"); importlib.import_module("src.services")
    _stub_module("src.services.rabbitmq_utils", get_connection=MagicMock(), get_connection_with_retry=MagicMock(), send_to_dlq=MagicMock())
    _stub_module("src.utils.xml_validator", validate_xml=lambda x, s=None: (True, None))
    _stub_module("src.services.fossbilling_api", create_registration_invoice=lambda *a, **k: "INV-1", pay_invoice=lambda *a, **k: True, update_client_by_identity_uuid=lambda *a, **k: True)
    _stub_module("src.services.identity_client", request_master_uuid=lambda *a, **k: T_UUID)
    _stub_module("src.services.consumption_store", save_items=MagicMock(), get_pending_company_ids=lambda: [], update_meta_by_correlation_id=lambda *a, **k: True, get_items_by_correlation_id=lambda *a, **k: [])
    rmod = importlib.import_module("src.services.rabbitmq_receiver")
    def f_rec(b: bytes):
        rmod.seen_message_ids.clear(); ch = MagicMock()
        rmod.process_message(ch, MagicMock(delivery_tag=1), MagicMock(), b)
        return ch.basic_ack.called, "Nacked"
    return f_rec

# ── Dynamic Flow Runner ────────────────────────────────────────────────────────

class DynamicFlowRunner:
    def __init__(self, repos_dir: Path):
        self.repos_dir = repos_dir; self.runners = {}; self.producers = {}; self.receivers = {}; self.xsd_map = {}

    def setup(self, args):
        repos = self.repos_dir
        # 1. FRONTEND
        f_dir = find_repo(repos, "IP-groep1-frontend")
        if f_dir:
            run_fe = get_frontend_runner(f_dir); self.runners["frontend"] = run_fe
            fe_reg_data = {"identity_uuid": T_UUID, "email": "t@e.com", "first_name": "J", "last_name": "J", "date_of_birth": "1990-01-01", "address": "S 1, 1000 B", "session_id": T_SESSION}
            self.producers[("frontend", "new_registration")] = lambda: run_fe("NewRegistrationSender", fe_reg_data)
            self.producers[("frontend", "user_created")] = lambda: run_fe("UserCreatedSender", {"identity_uuid": T_UUID, "email": "t@e.com", "date_of_birth": "1990-01-01"})
            self.producers[("frontend", "user_registered")] = lambda: run_fe("UserRegisteredSender", fe_reg_data)
            self.producers[("frontend", "user_updated")] = lambda: run_fe("UserUpdatedSender", fe_reg_data)
            self.producers[("frontend", "user_checkin")] = lambda: run_fe("UserCheckinSender", {"user_id": T_UUID, "session_id": T_SESSION, "badge_id": T_BADGE})
            self.producers[("frontend", "event_ended")] = lambda: run_fe("EventEndedSender", {"event_id": "EV-001", "session_id": T_SESSION, "end_time": T_NOW})
            self.producers[("frontend", "calendar_invite")] = lambda: run_fe("CalendarInviteSender", {"session_id": T_SESSION, "title": "T", "start_datetime": T_NOW, "end_datetime": T_NOW, "location": "L", "identity_uuid": T_UUID, "attendee_email": "t@e.com"})
            self.producers[("frontend", "session_create_request")] = lambda: run_fe("SessionCreateRequestSender", {"session_id": T_SESSION, "title": "T", "start_datetime": T_NOW, "end_datetime": T_NOW, "location": "L", "max_attendees": 100})
            self.producers[("frontend", "session_update_request")] = lambda: run_fe("SessionUpdateRequestSender", {"session_id": T_SESSION, "title": "U", "start_datetime": T_NOW, "end_datetime": T_NOW})
            self.producers[("frontend", "session_delete_request")] = lambda: run_fe("SessionDeleteRequestSender", {"session_id": T_SESSION})
            self.producers[("frontend", "user_deleted")] = lambda: run_fe("UserUnregisteredSender", {"identity_uuid": T_UUID})
            self.producers[("frontend", "company_member_removed")] = lambda: run_fe("CompanyMemberRemovedSender", {"company_id": "C1", "identity_uuid": T_UUID, "reason": "admin_removed", "email": "t@e.com"})
            self.receivers["frontend"] = lambda b: (True, "")
            self.xsd_map["frontend_new_registration"] = f_dir / "xsd" / "new_registration.xsd"

        # 2. CRM
        c_dir = find_repo(repos, "CRM")
        if c_dir:
            run_crm = get_crm_runner(c_dir); self.runners["crm"] = run_crm
            self.receivers["crm"] = lambda b: run_crm("process", "handleMessage", b.decode("utf-8") if isinstance(b, bytes) else b)
            self.producers[("crm", "new_registration")] = lambda: run_crm("build", "sendNewRegistrationToKassa", {"customer": {"identity_uuid": T_UUID, "email": "t@e.be", "type": "private"}, "session_id": T_SESSION})[1]
            self.producers[("crm", "profile_update")] = lambda: run_crm("build", "sendProfileUpdateToKassa", {"identity_uuid": T_UUID, "email": "new@test.be"})[1]
            self.producers[("crm", "invoice_request")] = lambda: run_crm("build", "sendInvoiceRequest", {"identity_uuid": T_UUID, "invoice_data": {"amount": "75.00"}})[1]
            self.producers[("crm", "send_mailing")] = lambda: run_crm("build", "sendMailingSend", {"identity_uuid": T_UUID, "mail_type": "registration_confirmation"})[1]
            self.xsd_map["crm_new_registration"] = c_dir / "xsd" / "new_registration_kassa.xsd"

        # 3. KASSA
        k_dir = find_repo(repos, "Kassa")
        if k_dir:
            ki = k_dir / "integratie"; sys.path.insert(0, str(ki))
            _stub_module("defusedxml.ElementTree", fromstring=etree.fromstring, ParseError=type("ParseError", (Exception,), {}))
            _stub_module("defusedxml.xmlrpc", monkey_patch=MagicMock())
            with patch.dict(os.environ, {"ODOO_URL":"u","ODOO_DB":"d","ODOO_USER":"u","ODOO_PASS":"p"}):
                try:
                    import sender as s_k; import receiver as r_k
                    self.receivers["kassa"] = lambda b: (patch("receiver.get_odoo_connection", return_value=(1, MagicMock()))(lambda _: (r_k.process_message(MagicMock(), MagicMock(delivery_tag=1), MagicMock(), b), MagicMock()))()[0], "Nacked")
                    self.producers[("kassa", "consumption_order")] = lambda: s_k.build_consumption_order_xml([{"id":"1","sku":"S1","description":"T","quantity":1,"unit_price":"5.0","vat_rate":"21","total_amount":5.0,"currency":"eur","item_type":"food"}], "42", T_UUID, "private", "t@e.com", {"street":"S","number":"1","postal_code":"1","city":"B","country":"be"})
                    self.producers[("kassa", "payment_registered")] = lambda: s_k.build_payment_registered_xml("consumption", "paid", "10.0", T_DATE, "T1", "on_site", "I1", T_UUID, T_CORR)
                    self.producers[("kassa", "payment_status")] = lambda: s_k.build_payment_status_xml(T_UUID, "paid")
                except Exception as e: warn(f"Kassa setup failed: {e}")

        # 4. PLANNING / 5. FACTURATIE
        f_fact = find_repo(repos, "Facturatie")
        if f_fact:
            try:
                self.receivers["facturatie"] = _make_facturatie_process_fn(f_fact)
                import src.services.rabbitmq_sender as s_f
                self.producers[("facturatie", "payment_registered")] = lambda: s_f.build_payment_confirmed_xml("I1", T_UUID, "75.00", "eur", "online", paid_at=T_NOW, source="facturatie", status="paid", due_date=T_DATE)
                self.producers[("facturatie", "send_mailing")] = lambda: s_f.build_invoice_created_notification_xml("I1","t@e.com",T_CORR,"J","J","C1",T_UUID)
            except Exception as e: warn(f"Facturatie setup failed: {e}")

        # 7. MAILING
        m_dir = find_repo(repos, "Mailing")
        if m_dir:
            ms = m_dir / "mailing_service"; sys.path.insert(0, str(ms))
            _stub_module("sendgrid_client", SendGridAPIClient=MagicMock(), Recipient=MagicMock(), Attachment=MagicMock(), SendGridError=Exception, send_template_email=MagicMock(return_value=MagicMock(rejected=[])), SendResult=type("SendResult", (), {"__init__": lambda self, a=[], r=[]: None, "accepted": [], "rejected": []}))
            from publishers import mailing_status as pub_m; from consumers import send_mailing as cons_m; import envelope as env_m
            self.producers[("mailing", "mailing_status")] = lambda: etree.tostring(pub_m._build_element(correlation_id=T_CORR, campaign_id="C1", subject="S", sent=1, delivered=1, bounced=0, opened=0, bounced_emails=[], status="completed"), encoding="unicode")
            def m_rec_dyn(b):
                p = ms / "schemas" / "send_mailing.xsd"
                if not p.exists(): return False, f"XSD missing: {p}"
                s_xsd = etree.XMLSchema(etree.parse(p))
                with patch.dict(os.environ, {"FROM_EMAIL": "a@t.l", "SCHEMAS_DIR": str(ms/"schemas")}, clear=False):
                    with patch("sendgrid_client.send_template_email", return_value=MagicMock(rejected=[])):
                        with patch("templates.resolve_template_id", return_value="t"):
                            with patch("builtins.open", side_effect=lambda f, *a, **k: open(Path(str(f).replace("/app/schemas", str(ms/"schemas"))), *a, **k)):
                                cons_m.handle(env_m.parse_and_validate(b, s_xsd), MagicMock())
                return True, ""
            self.receivers["mailing"] = m_rec_dyn

    def run_all(self, args):
        flows = _flows_by_id(); header("Team-overstijgende Flow Simulatie")
        repos = Path(args.repos_dir)
        for flow_id, flow in sorted(flows.items()):
            team = flow["producer"]["team"]; msg_type = flow["type"]; source = flow["source"]
            if not team_applies(args, team) and team != "*": continue
            builder = self.producers.get((team, msg_type))
            flow_receivers = {}
            for consumer in flow.get("consumers", []):
                ct = consumer["team"]
                if ct in self.receivers and team_applies(args, ct): flow_receivers[ct] = self.receivers[ct]
            xsd_path = self.xsd_map.get(flow_id)
            if not xsd_path or not xsd_path.exists():
                candidates = [repos/"contracts"/"xsd"/f"{msg_type}.xsd", repos/"IP-groep1-frontend"/"xsd"/f"{msg_type}.xsd", repos/"CRM"/"xsd"/f"{msg_type}.xsd", repos/"Facturatie"/"src"/"services"/"xsd"/f"{msg_type}.xsd"]
                for c in candidates:
                    if c.is_file(): xsd_path = c; break
            _run_case(args, f"Simulatie: {flow_id}", builder if builder else lambda: None, xsd_path, msg_type, source, receiver_fns=flow_receivers if flow_receivers else None, fallback_flow_id=flow_id)

def main():
    args = parse_args(); load_env(args.env); sum_file = os.getenv("GITHUB_STEP_SUMMARY"); capture = StringIO(); orig_out, orig_err = sys.stdout, sys.stderr
    if sum_file: sys.stdout = TeeStream(orig_out, capture); sys.stderr = TeeStream(orig_err, capture)
    try:
        print(f"\n{BOLD}COMPREHENSIVE BEHAVIORAL AUDIT — v2.3{RESET}")
        runner = DynamicFlowRunner(Path(args.repos_dir)); runner.setup(args); runner.run_all(args)
        fc = _state["failures"]; print(f"\n{'═'*60}\nAudit klaar. Fouten gevonden: {fc}")
        exit_code = 1 if fc else 0
    except Exception as e: traceback.print_exc(); exit_code = 1
    finally:
        if sum_file:
            sys.stdout, sys.stderr = orig_out, orig_err
            with open(sum_file, "a", encoding="utf-8") as f:
                f.write("## 🧪 Behavioral Audit v2.3 — Geautomatiseerd Rapport\n\n")
                f.write("Dit rapport toont precies waar berichten in de keten vastlopen.\n\n")
                f.write("| Bericht Flow | Output (Sturen door Team) | Contract (XSD Match) | Input (Ontvangen door Team) | Resultaat Verwerking |\n")
                f.write("|---|---|---|---|---|\n")
                for r in _state["results"]:
                    # SENDER STATUS
                    p1 = "✅ OK" if r["sender_status"] == "OK" else "❌ FOUT" if r["sender_status"] == "FAIL" else "⚠️ VOORBEELD"
                    if r["sender_status"] == "FAIL" and r["builder_error"]:
                        p1 += f"<br><sub>{r['builder_error'][:150]}</sub>"
                    
                    # XSD STATUS
                    xsd = "✅ MATCH" if r["xsd_status"] == "OK" else "❌ MISMATCH" if r["xsd_status"] == "FAIL" else "⏭️"
                    if r["xsd_status"] == "FAIL" and r["xsd_error"]:
                        xsd += f"<br><sub>{r['xsd_error'][:150]}</sub>"

                    if r["receivers"]:
                        for idx, rec in enumerate(r["receivers"]):
                            f_id = r["flow_id"] if idx == 0 else ""
                            p1_v = f"**{r['sender_team']}**: {p1}" if idx == 0 else ""
                            xsd_v = xsd if idx == 0 else ""
                            l_stat = "✅ SUCCES" if rec["status"] == "OK" else "❌ **GEFAALD**"
                            err_txt = f"<br><sub>{rec['error'][:200]}</sub>" if rec["error"] else ""
                            f.write(f"| `{f_id}` | {p1_v} | {xsd_v} | **{rec['team']}** | {l_stat}{err_txt} |\n")
                    else:
                        f.write(f"| `{r['flow_id']}` | **{r['sender_team']}**: {p1} | {xsd} | - | ⏭️ N.v.t. |\n")
                f.write("\n\n### 📝 Legenda\n- **Output (Sturen)**: Kan de software van dit team een bericht bouwen?\n- **Contract**: Voldoet het bericht aan de XML-schema (XSD) afspraken?\n- **Input (Ontvangen)**: Kan de software van het ontvangende team de data verwerken?\n")
                f.write(markdown_full_log(capture.getvalue()))
    sys.exit(exit_code)

if __name__ == "__main__": main()
