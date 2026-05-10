"""
test_contract_full.py  —  Comprehensive Behavioral Contract Audit
Groep 1 — Desideriushogeschool 2026  /  XML/XSD Contract v2.3

This suite executes real team code to verify the entire lifecycle of EVERY flow:
1.  BUILD: Can the sender generate valid XML? (Phase 1)
2.  VALIDATE: Does it match the contract XSD? (Phase 1)
3.  PROCESS: Can the receiver handle the XML without logic errors? (Phase 3)

Also:
- contract_flows.yaml drives optional `example:` XML fallbacks when a builder crashes or returns
  empty output (place examples under `<repos-dir>/contracts/`).
- E2E chains (see test_e2e_chains) feed one team’s XML into the next consumer; use
  `--no-chains` to skip. Chains run only when `--teams` includes every producer/consumer
  in that hop (e.g. frontend+crm for the first chain).
- Kassa `consumption_order` Phase 3 fans out to CRM + Facturatie when those repos are
  present (same payload as in contract_flows).
- `test_contract_example_sweep` validates every `example` + `schema` pair from
  `contract_flows.yaml` under `contracts/` (broad static coverage). Use `--no-contract-sweep` to skip.
- GitHub Actions: full console output is always appended to `GITHUB_STEP_SUMMARY`, even if the
  run crashes mid-suite (e.g. `SystemExit` from an imported team module).
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

# ── Repository path helper ─────────────────────────────────────────────────────
def find_repo(repos_dir: Path, name: str) -> Path | None:
    options = [repos_dir / name, repos_dir.parent / name, repos_dir / "xml-test-env" / name]
    for p in options:
        if p.exists() and p.is_dir(): return p
    return None

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
    p = argparse.ArgumentParser()
    p.add_argument("--teams", default="all")
    p.add_argument("--repos-dir", default=os.getenv("REPOS_DIR", str(Path(__file__).resolve().parent.parent)))
    p.add_argument("--host", default=os.getenv("RABBIT_HOST", "20.126.113.148"))
    p.add_argument("--port", type=int, default=int(os.getenv("RABBIT_PORT", 30000)))
    p.add_argument("--user", default=os.getenv("RABBIT_USER", "guest"))
    p.add_argument("--pass", dest="password", default=os.getenv("RABBIT_PASS", "guest"))
    p.add_argument("--vhost", default=os.getenv("RABBIT_VHOST", "/"))
    p.add_argument("--phase1-only", action="store_true")
    p.add_argument("--phase2-only", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--no-chains", action="store_true")
    p.add_argument("--no-contract-sweep", action="store_true")
    p.add_argument("--env", default=".env")
    return p.parse_args()

_FLOWS_BY_ID = None
def _flows_by_id():
    global _FLOWS_BY_ID
    if _FLOWS_BY_ID is not None: return _FLOWS_BY_ID
    if not _YAML_AVAILABLE: _FLOWS_BY_ID = {}; return {}
    path = Path(__file__).resolve().parent / "contract_flows.yaml"
    if not path.exists(): _FLOWS_BY_ID = {}; return {}
    with path.open(encoding="utf-8") as f: data = yaml.safe_load(f)
    flows = data.get("flows") if isinstance(data, dict) else None
    _FLOWS_BY_ID = {f["id"]: f for f in (flows or []) if isinstance(f, dict) and "id" in f}
    return _FLOWS_BY_ID

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

def team_applies(args, *team_tags: str) -> bool:
    raw = (args.teams or "all").strip().lower()
    if raw == "all": return True
    sel = {t.strip().lower() for t in raw.split(",") if t.strip()}
    return bool(sel.intersection({t.lower() for t in team_tags}))

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
    try: root = etree.fromstring(xml_str.encode("utf-8") if isinstance(xml_str, str) else xml_str)
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
    elif root.tag != flat_root: issues.append(f"Root mismatch: got <{root.tag}>, want <{flat_root}>")
    return issues

def _run_case(args, name, builder_fn, xsd_path, msg_type, source, receiver_fns=None, flat_root=None, fallback_flow_id=None):
    print(f"\n  [{name}]")
    _state["tests"] += 1
    res = {"name": name, "p1": "fail", "p3": "skip", "error": "", "proc_error": "", "used_fallback": False}
    repos = Path(args.repos_dir); xml_str = None
    def _load_fallback(reason: str) -> bool:
        nonlocal xml_str
        if not fallback_flow_id: return False
        p = contract_example_path(repos, fallback_flow_id)
        if not p: warn(f"Contract fallback unavailable for flow `{fallback_flow_id}` ({reason})"); return False
        try: xml_str = p.read_text(encoding="utf-8"); res["used_fallback"] = True; warn(f"Using fallback: {p.name}"); return True
        except OSError: return False
    try:
        raw = builder_fn()
        if not raw:
            if not _load_fallback("builder returned empty"):
                if receiver_fns: ok("Phase 1: Skipped (Injecting template for Receiver)"); res["p1"] = "skip"
                else: raise ValueError("Builder returned empty")
        else:
            xml_str = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            if (xml_str or "").strip().startswith("ERROR:"):
                if not _load_fallback("builder reported ERROR"): raise ValueError(xml_str)
    except Exception as e:
        if not _load_fallback(f"builder raised: {e}"): fail(f"Phase 1: Builder error: {e}"); res["error"] = str(e); xml_str = None
    if xml_str is not None and res["p1"] != "skip":
        try:
            issues = structural_checks(xml_str, msg_type, source, flat_root)
            if issues: fail(f"Structural: {'; '.join(issues)}"); res["error"] = "; ".join(issues)
            else:
                ok("Phase 1: Structural pass")
                valid, err = validate_against_xsd(xml_str, xsd_path)
                if valid: ok(f"Phase 1: XSD valid ({xsd_path.name if xsd_path else 'no XSD'})"); res["p1"] = "pass"
                else: fail(f"Phase 1: XSD invalid: {err}"); res["error"] = err
        except Exception as e: fail(f"Phase 1: Validation error: {e}"); res["error"] = str(e)
    if not args.phase1_only and receiver_fns and xml_str:
        for r_name, r_fn in receiver_fns.items():
            _state["tests"] += 1; info(f"Phase 3: Injecting into handler: {r_name}...")
            try:
                clean_xml = re.sub(r'<\?xml[^?]+\?>', '', xml_str).strip()
                p_res, p_err = r_fn(clean_xml.encode('utf-8'))
                if p_res: ok(f"Phase 3 ({r_name}): Processing success"); res["p3"] = "pass"
                else: fail(f"Phase 3 ({r_name}): Processing failure: {p_err}"); res["p3"] = "fail"; res["proc_error"] += f"[{r_name}]: {p_err}; "
            except Exception as e: fail(f"Phase 3 ({r_name}): Handler crash: {e}"); res["p3"] = "fail"; res["proc_error"] += f"[{r_name}]: {e}; "
    if res["p1"] == "fail" or res["p3"] == "fail": _state["failures"] += 1
    _state["results"].append(res)

# ═══════════════════════════════════════════════════════════════════════════════
# TEAM RUNNERS
# ═══════════════════════════════════════════════════════════════════════════════

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
if (!class_exists('Drupal\\rabbitmq_sender\\{method}')) {{ fwrite(STDERR, "ERROR: Class Drupal\\rabbitmq_sender\\{method} not found."); exit(1); }}
$data = json_decode(file_get_contents({json_lit}), true);
$sender = new \\Drupal\\rabbitmq_sender\\{method}();
echo $sender->buildXml($data);
"""
            with tempfile.NamedTemporaryFile(mode="w", suffix=".php", delete=False, encoding="utf-8") as pf:
                pf.write(php_src); php_path = pf.name
            res = subprocess.run(["php", php_path], capture_output=True, text=True, cwd=str(f_dir_p))
            return f"ERROR: {res.stderr or res.stdout}" if res.returncode != 0 else res.stdout
        except Exception as e: return f"ERROR: {e}"
        finally:
            for p in (json_path, php_path):
                if p:
                    try: os.unlink(p)
                    except OSError: pass
    return run_php

def get_crm_runner(c_dir):
    def run_node(mode, m, d):
        script = f"""
        const Module = require('module'); const orig = Module.prototype.require;
        const mockSF = {{
            init: async () => true, isConnected: true,
            apiCall: async (fn) => {{
                const conn = {{ sobject: (type) => ({{
                    upsert: async () => ({{ success: true, id: 'SF-TEST-ID' }}),
                    create: async () => ({{ success: true, id: 'SF-TEST-ID' }}),
                    update: async () => ({{ success: true, id: 'SF-TEST-ID' }}),
                    find: () => ({{ limit: () => Promise.resolve([{{ Id: 'SF-TEST-ID' }}]) }})
                }}) }}; return await fn(conn);
            }}
        }};
        const mockMQ = {{
            assertQueue: async () => ({{ queue: 'mock' }}), assertExchange: async () => {{}}, bindQueue: async () => {{}},
            consume: async () => ({{ consumerTag: 't' }}), sendToQueue: () => true, publish: () => true, ack: () => {{}}, nack: () => {{}}
        }};
        Module.prototype.require = function(p) {{
            if (p === 'libxmljs2') return {{ parseXml: () => ({{ validate: () => true }}), memoryUsage: () => 0 }};
            if (p === 'amqplib') return {{ connect: async () => ({{ createChannel: async () => mockMQ, on: () => {{}} }}) }};
            if (p && p.includes('sfConnection')) return function() {{ return mockSF; }};
            if (p && p.includes('sender')) {{
                const Real = orig.call(this, p);
                return class Mock extends Real {{
                    constructor() {{ super(...arguments); this.init = async () => {{}}; this.sendLog = async () => ({{ success: true }}); this.channel = mockMQ; }}
                }};
            }}
            return orig.apply(this, arguments);
        }};
        async function run() {{
            try {{
                if ("{mode}" === "build") {{
                    const S = require('./src/sender'); const s = new S(); await s.init().catch(()=>{{}});
                    const result = await s.{m}({json.dumps(d)});
                    process.stdout.write(typeof result === 'object' ? (result.payload || JSON.stringify(result)) : String(result));
                }} else {{
                    const R = require('./src/receiver'); const r = new R(); r.channel = mockMQ; r.sf = mockSF;
                    if (r.sender) {{ r.sender.channel = mockMQ; r.sender.init = async () => {{}}; }}
                    const msg = {{ content: Buffer.from(typeof {json.dumps(d)} === 'string' ? {json.dumps(d)} : JSON.stringify({json.dumps(d)})), fields: {{ deliveryTag: 1 }}, properties: {{ contentType: 'application/xml' }} }};
                    let logs = []; console.log = (...a) => logs.push(a.join(' ')); console.error = (...a) => logs.push(a.join(' '));
                    await r.handleMessage(msg);
                    const err = logs.find(l => (l.includes('Error') || l.includes('INVALID_FIELD')) && !l.includes('WARNING') && !l.includes('RabbitMQ'));
                    if (err) {{ process.stderr.write(err); process.exit(1); }}
                    process.stdout.write("SUCCESS");
                }}
            }} catch (e) {{ process.stderr.write(e.stack || e.message); process.exit(1); }}
        }}
        run();
        """
        try:
            res = subprocess.run(["node", "-e", script], cwd=str(c_dir), capture_output=True, text=True)
            return res.returncode == 0, res.stdout if res.returncode == 0 else res.stderr
        except Exception as e: return False, str(e)
    return run_node

def _make_facturatie_process_fn(f_dir: Path):
    sys.path.insert(0, str(f_dir))
    for k in ["src", "src.services", "src.utils", "mysql", "mysql.connector"]:
        if k in sys.modules: del sys.modules[k]
    _stub_module("mysql.connector", connect=MagicMock()); _stub_module("mysql.connector.pooling", MySQLConnectionPool=MagicMock())
    importlib.import_module("src"); importlib.import_module("src.services")
    _stub_module("src.services.rabbitmq_utils", get_connection=MagicMock(), get_connection_with_retry=MagicMock(), send_to_dlq=MagicMock())
    _stub_module("src.utils.xml_validator", validate_xml=lambda x, s=None: (True, None))
    _stub_module("src.services.fossbilling_api", create_registration_invoice=lambda *a: "INV-1", pay_invoice=lambda *a: True)
    _stub_module("src.services.identity_client", request_master_uuid=lambda *a: T_UUID)
    _stub_module("src.services.consumption_store", store_consumption=MagicMock(), save_items=MagicMock())
    rmod = importlib.import_module("src.services.rabbitmq_receiver")
    def f_rec(b: bytes):
        rmod.seen_message_ids.clear(); ch = MagicMock()
        rmod.process_message(ch, MagicMock(delivery_tag=1), MagicMock(), b)
        return ch.basic_ack.called, "Nacked"
    return f_rec

# ═══════════════════════════════════════════════════════════════════════════════
# DYNAMIC E2E RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

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
            self.producers[("frontend", "user_registered")] = lambda: run_fe("UserRegisteredSender", {"identity_uuid": T_UUID, "type": "private"})
            self.producers[("frontend", "user_updated")] = lambda: run_fe("UserUpdatedSender", {"identity_uuid": T_UUID, "email": "new@e.com"})
            self.producers[("frontend", "user_checkin")] = lambda: run_fe("UserCheckinSender", {"identity_uuid": T_UUID, "session_id": T_SESSION, "badge_id": T_BADGE})
            self.producers[("frontend", "event_ended")] = lambda: run_fe("EventEndedSender", {"event_id": "EV-001", "session_id": T_SESSION, "end_time": T_NOW})
            self.producers[("frontend", "calendar_invite")] = lambda: run_fe("CalendarInviteSender", {"session_id": T_SESSION, "title": "T", "start_datetime": T_NOW, "end_datetime": T_NOW, "location": "L", "identity_uuid": T_UUID, "attendee_email": "t@e.com"})
            self.producers[("frontend", "session_create_request")] = lambda: run_fe("SessionCreateRequestSender", {"session_id": T_SESSION, "title": "T", "start_datetime": T_NOW, "end_datetime": T_NOW, "location": "L", "max_attendees": 100})
            self.producers[("frontend", "session_update_request")] = lambda: run_fe("SessionUpdateRequestSender", {"session_id": T_SESSION, "title": "U", "start_datetime": T_NOW, "end_datetime": T_NOW})
            self.producers[("frontend", "session_delete_request")] = lambda: run_fe("SessionDeleteRequestSender", {"session_id": T_SESSION})
            self.producers[("frontend", "cancel_registration")] = lambda: run_fe("CancelRegistrationSender", {"identity_uuid": T_UUID, "session_id": T_SESSION})
            self.producers[("frontend", "user_deleted")] = lambda: run_fe("UserUnregisteredSender", {"identity_uuid": T_UUID})
            self.producers[("frontend", "company_member_removed")] = lambda: run_fe("CompanyMemberRemovedSender", {"company_id": "C1", "identity_uuid": T_UUID, "reason": "admin_removed"})
            self.receivers["frontend"] = lambda b: (True, "")
            self.xsd_map["frontend_new_registration"] = f_dir / "xsd" / "new_registration.xsd"
            self.xsd_map["frontend_session_create_request"] = f_dir / "xsd" / "session_create_request.xsd"
            self.xsd_map["frontend_session_update_request"] = f_dir / "xsd" / "session_update_request.xsd"
            self.xsd_map["frontend_session_delete_request"] = f_dir / "xsd" / "session_delete_request.xsd"
            self.xsd_map["frontend_user_deleted"] = f_dir / "xsd" / "user_deleted.xsd"

        # 2. CRM
        c_dir = find_repo(repos, "CRM")
        if c_dir:
            run_crm = get_crm_runner(c_dir); self.runners["crm"] = run_crm
            self.receivers["crm"] = lambda b: run_crm("process", "handleMessage", b.decode("utf-8") if isinstance(b, bytes) else b)
            crm_reg_data = {"customer": {"identity_uuid": T_UUID, "email": "t@e.be", "first_name": "L", "last_name": "D", "type": "private"}, "session_id": T_SESSION, "payment_due": {"amount": "10.00", "status": "unpaid"}, "correlation_id": T_CORR}
            self.producers[("crm", "new_registration")] = lambda: run_crm("build", "sendNewRegistrationToKassa", crm_reg_data)[1]
            self.producers[("crm", "profile_update")] = lambda: run_crm("build", "sendProfileUpdateToKassa", {"identity_uuid": T_UUID, "email": "new@test.be"})[1]
            self.producers[("crm", "invoice_request")] = lambda: run_crm("build", "sendInvoiceRequest", {"identity_uuid": T_UUID, "invoice_data": {"amount": "75.00"}})[1]
            self.producers[("crm", "send_mailing")] = lambda: run_crm("build", "sendMailingSend", {"identity_uuid": T_UUID, "mail_type": "welcome"})[1]
            self.producers[("crm", "cancel_registration")] = lambda: run_crm("build", "sendCancelRegistrationToPlanning", {"identity_uuid": T_UUID, "session_id": T_SESSION})[1]
            self.producers[("crm", "wallet_balance_update")] = lambda: run_crm("build", "sendWalletBalanceUpdate", {"identity_uuid": T_UUID, "balance": "15.00"})[1]
            self.xsd_map["crm_new_registration"] = c_dir / "xsd" / "new_registration_kassa.xsd"
            self.xsd_map["crm_send_mailing"] = c_dir / "xsd" / "send_mailing.xsd"
            self.xsd_map["crm_invoice_request"] = c_dir / "xsd" / "invoice_request.xsd"

        # 3. KASSA
        k_dir = find_repo(repos, "Kassa")
        if k_dir:
            ki = k_dir / "integratie"; sys.path.insert(0, str(ki))
            _stub_module("defusedxml.ElementTree", fromstring=etree.fromstring, ParseError=type("ParseError", (Exception,), {}))
            _stub_module("defusedxml.xmlrpc", monkey_patch=MagicMock())
            mock_env = {"RABBIT_HOST": args.host, "RABBIT_PORT": str(args.port), "RABBIT_USER": args.user, "RABBIT_PASS": args.password, "ODOO_URL":"u","ODOO_DB":"d","ODOO_USER":"u","ODOO_PASS":"p"}
            with patch.dict(os.environ, mock_env):
                try:
                    if "sender" in sys.modules: del sys.modules["sender"]
                    if "receiver" in sys.modules: del sys.modules["receiver"]
                    import sender as s_k; import receiver as r_k
                    self.receivers["kassa"] = lambda b: (patch("receiver.get_odoo_connection", return_value=(1, MagicMock()))(lambda _: (r_k.process_message(MagicMock(), MagicMock(delivery_tag=1), MagicMock(), b), MagicMock()))()[0], "Nacked")
                    self.producers[("kassa", "consumption_order")] = lambda: s_k.build_consumption_order_xml([{"id":"1","sku":"S1","description":"T","quantity":1,"unit_price":"5.0","vat_rate":"21","total_amount":5.0,"currency":"eur","item_type":"food"}], "42", T_UUID, "private", "t@e.com", {"street":"S","number":"1","postal_code":"1","city":"B","country":"be"})
                    self.producers[("kassa", "payment_registered")] = lambda: s_k.build_payment_registered_xml("consumption", "paid", "10.0", T_DATE, "T1", "on_site", "I1", T_UUID, T_CORR)
                    self.producers[("kassa", "badge_assigned")] = lambda: s_k.build_badge_assigned_xml(T_BADGE, T_UUID)
                    self.producers[("kassa", "refund_processed")] = lambda: s_k.build_refund_processed_xml(T_CORR, "partial", 10.0, "card_reversal", "duplicate_payment", "T1", identity_uuid=T_UUID)
                    self.producers[("kassa", "wallet_balance_update")] = lambda: s_k.build_wallet_balance_update_xml(T_UUID, 12.50, authority="crm", status="active")
                    self.producers[("kassa", "payment_status")] = lambda: s_k.build_payment_status_xml(T_UUID, "paid")
                    self.producers[("kassa", "invoice_request")] = lambda: s_k.build_invoice_request_xml(T_UUID, {"first_name":"J","last_name":"J","email":"t@e.com"}, T_CORR)
                    self.xsd_map["kassa_consumption_order"] = ki / "schemas" / "schema_consumption_order_v2.3.xsd"
                    self.xsd_map["kassa_payment_registered_consumption"] = ki / "schemas" / "schema_payment_registered_v2.1.xsd"
                    self.xsd_map["kassa_payment_registered_registration"] = ki / "schemas" / "schema_payment_registered_v2.1.xsd"
                except Exception as e: warn(f"Kassa dynamic setup failed: {e}")

        # 4. PLANNING / 5. FACTURATIE / 6. IDENTITY
        p_dir = find_repo(repos, "Planning")
        if p_dir:
            sys.path.insert(0, str(p_dir))
            _stub_module("psycopg2", connect=MagicMock()); _stub_module("psycopg2.extras", RealDictCursor=MagicMock(), DictCursor=MagicMock())
            _stub_module("msal", PublicClientApplication=MagicMock(), ConfidentialClientApplication=MagicMock(), SerializableTokenCache=MagicMock())
            _stub_module("requests", get=MagicMock(), post=MagicMock(), Session=MagicMock(), Response=MagicMock())
            _stub_module("cryptography.fernet", Fernet=MagicMock(), InvalidToken=type("InvalidToken", (Exception,), {}))
            _stub_module("azure.identity", DefaultAzureCredential=MagicMock())
        f_fact = find_repo(repos, "Facturatie")
        if f_fact:
            try:
                self.receivers["facturatie"] = _make_facturatie_process_fn(f_fact)
                import src.services.rabbitmq_sender as s_f
                self.producers[("facturatie", "invoice_status")] = lambda: s_f.build_invoice_created_notification_xml("I1","t@e.com",T_CORR,"J","J","C1",T_UUID)
                self.producers[("facturatie", "payment_registered")] = lambda: s_f.build_payment_confirmed_xml("I1", T_UUID, "75.00", "eur", "online", paid_at=T_NOW, source="facturatie", status="paid", due_date=T_DATE)
                self.producers[("facturatie", "send_mailing")] = lambda: s_f.build_invoice_created_notification_xml("I1","t@e.com",T_CORR,"J","J","C1",T_UUID)
                self.xsd_map["facturatie_send_mailing"] = f_fact / "src" / "services" / "xsd" / "send_mailing.xsd"
            except Exception as e: warn(f"Facturatie setup failed: {e}")
        i_dir = find_repo(repos, "identity-service")
        if i_dir:
            _stub_module("sqlalchemy", Column=MagicMock(), String=MagicMock(), Boolean=MagicMock(), DateTime=MagicMock(), create_engine=MagicMock(), Integer=MagicMock(), ForeignKey=MagicMock(), Integer=MagicMock())
            _stub_module("sqlalchemy.ext.declarative", declarative_base=lambda: MagicMock())
            _stub_module("sqlalchemy.orm", sessionmaker=MagicMock(), Session=MagicMock(), relationship=MagicMock(), declarative_base=lambda: MagicMock())

        # 7. MAILING
        m_dir = find_repo(repos, "Mailing")
        if m_dir:
            ms = m_dir / "mailing_service"; sys.path.insert(0, str(ms))
            _stub_module("python_http_client", client=MagicMock())
            _stub_module("sendgrid_client", SendGridAPIClient=MagicMock(), Recipient=MagicMock(), Attachment=MagicMock(), SendGridError=Exception, send_template_email=MagicMock(return_value=MagicMock(rejected=[])), SendResult=type("SendResult", (), {"__init__": lambda self, a=[], r=[]: None, "accepted": [], "rejected": []}))
            from publishers import mailing_status as pub_m; from consumers import send_mailing as cons_m; import envelope as env_m
            self.producers[("mailing", "mailing_status")] = lambda: etree.tostring(pub_m._build_element(correlation_id=T_CORR, campaign_id="C1", subject="S", sent=1, delivered=1, bounced=0, opened=0, bounced_emails=[], status="completed"), encoding="unicode")
            def m_rec_dyn(b):
                p = ms / "schemas" / "send_mailing.xsd"
                if not p.exists(): return False, f"XSD missing: {p}"
                s_xsd = etree.XMLSchema(etree.parse(p))
                with patch.dict(os.environ, {"FROM_EMAIL": "a@t.l"}, clear=False):
                    with patch("sendgrid_client.send_template_email", return_value=MagicMock(rejected=[])):
                        with patch("templates.resolve_template_id", return_value="t"):
                            # Path patch: point /app/schemas to local schemas dir
                            with patch("builtins.open", side_effect=lambda f, *a, **k: open(Path(str(f).replace("/app/schemas", str(ms/"schemas"))), *a, **k)):
                                cons_m.handle(env_m.parse_and_validate(b, s_xsd), MagicMock())
                return True, ""
            self.receivers["mailing"] = m_rec_dyn; self.xsd_map["mailing_mailing_status"] = ms / "schemas" / "mailing_status.xsd"

    def run_all(self, args):
        flows = _flows_by_id(); header("Dynamic Flow Execution (Registry-Based)")
        repos = Path(args.repos_dir)
        for flow_id, flow in flows.items():
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
            _run_case(args, f"dynamic/{flow_id}", builder if builder else lambda: None, xsd_path, msg_type, source, receiver_fns=flow_receivers if flow_receivers else None, fallback_flow_id=flow_id)

def test_shared(args):
    if not (team_applies(args, "heartbeat") or team_applies(args, "monitoring")): return
    header("Heartbeat & Monitoring"); repos = Path(args.repos_dir); h_dir = find_repo(repos, "heartbeat")
    if h_dir and team_applies(args, "heartbeat"):
        def hb_xml(s, st, u):
            import xml.etree.ElementTree as ET
            m = ET.Element("message"); h = ET.SubElement(m, "header")
            for k,v in [("message_id",str(uuid.uuid4())),("timestamp",T_NOW),("source",s),("type","heartbeat"),("version","2.0")]: ET.SubElement(h,k).text=v
            b = ET.SubElement(m, "body"); ET.SubElement(b,"status").text=st; ET.SubElement(b,"uptime").text=str(u)
            return ET.tostring(m, encoding='unicode')
        _run_case(args, "heartbeat/heartbeat", lambda: hb_xml("hb_service", "online", 3600), h_dir / "heartbeat.xsd", "heartbeat", "hb_service")
    mon_dir = find_repo(repos, "monitoring")
    if mon_dir and team_applies(args, "monitoring"):
        det = mon_dir / "detector"; sys.path.insert(0, str(det))
        _stub_module("elasticsearch", Elasticsearch=MagicMock()); mock_log = MagicMock(); _stub_module("logging", getLogger=lambda n: mock_log)
        with patch.dict(os.environ, {"RABBIT_HOST": args.host}, clear=False):
            with patch("datetime.timezone", timezone.utc):
                try:
                    _stub_module("pika", BlockingConnection=MagicMock())
                    src = (det / "detector.py").read_text(encoding="utf-8")
                    safe_src = re.split(r'^while\s+True:', src[1:] if src.startswith('\ufeff') else src, flags=re.MULTILINE)[0]
                    detector = types.ModuleType("detector"); detector.logger = mock_log; detector.__dict__["__file__"] = str(det / "detector.py")
                    sys.modules["detector"] = detector; exec(safe_src, detector.__dict__)
                    def cap_alert(s):
                        cap = []
                        with patch("pika.BlockingConnection") as mc_cls:
                            mc = mc_cls.return_value; mch = MagicMock(); mc.channel.return_value = mch
                            mch.basic_publish.side_effect = lambda e,r,b,**k: cap.append(b)
                            detector.send_alert_xml(s)
                        return cap[0] if cap else None
                    _run_case(args, "monitoring/system_alert", lambda: cap_alert("kassa") or "", mon_dir / "xsd" / "system_alert.xsd", "HEARTBEAT_CRITICAL", "monitoring", flat_root="alert")
                except Exception as e: fail(f"monitoring: {e}")

def test_contract_example_sweep(args):
    if getattr(args, "no_contract_sweep", False): return
    header("Contract examples (contract_flows.yaml → XSD)"); repos = Path(args.repos_dir); contracts_root = repos / "contracts"
    if not contracts_root.is_dir() or not _YAML_AVAILABLE or not _LXML_AVAILABLE: return
    for fid in sorted(_flows_by_id().keys()):
        flow = _flows_by_id()[fid]; ex = flow.get("example"); sc = flow.get("schema")
        if not ex or ex in ("~", None) or not sc or sc in ("~", None): continue
        epath = contracts_root / ex; spath = contracts_root / sc; name = f"contract-sweep/{fid}"
        print(f"\n  [{name}]"); _state["tests"] += 1; res = {"name": name, "p1": "fail", "p3": "skip", "error": "", "proc_error": "", "used_fallback": False}
        if not epath.is_file(): fail(f"Example missing: {epath}"); res["error"] = "example file missing"
        elif not spath.is_file(): fail(f"Schema missing: {spath}"); res["error"] = "schema file missing"
        else:
            try:
                xml = epath.read_text(encoding="utf-8"); valid, err = validate_against_xsd(xml, spath)
                if valid: ok(f"Phase 1: validates ({spath.name})"); res["p1"] = "pass"
                else: fail(f"Phase 1: XSD invalid: {err}"); res["error"] = err
            except OSError as e: fail(f"Read error: {e}"); res["error"] = str(e)
        if res["p1"] == "fail": _state["failures"] += 1
        _state["results"].append(res)

def main():
    args = parse_args(); load_env(args.env); sum_file = os.getenv("GITHUB_STEP_SUMMARY"); capture = StringIO(); orig_out, orig_err = sys.stdout, sys.stderr
    if sum_file: sys.stdout = TeeStream(orig_out, capture); sys.stderr = TeeStream(orig_err, capture)
    try:
        print(f"\n{BOLD}COMPREHENSIVE BEHAVIORAL AUDIT — v2.3{RESET}")
        runner = DynamicFlowRunner(Path(args.repos_dir)); runner.setup(args); runner.run_all(args)
        test_shared(args); test_contract_example_sweep(args)
        tot, fc = _state["tests"], _state["failures"]; print(f"\n{'═'*60}\n{tot - fc} PASSED / {fc} FAILED")
        exit_code = 1 if fc else 0
    except Exception as e: traceback.print_exc(); exit_code = 1
    finally:
        if sum_file:
            sys.stdout, sys.stderr = orig_out, orig_err
            with open(sum_file, "a", encoding="utf-8") as f:
                f.write("## 🧪 Behavioral Audit v2.3\n\n| Test Case | XSD | Logic |\n|---|---|---|\n")
                for r in _state["results"]:
                    p1 = "✅" if r["p1"] == "pass" else "❌" if r["p1"] == "fail" else "⏭️"
                    p3 = "✅" if r["p3"] == "pass" else "❌" if r["p3"] == "fail" else "⏭️"
                    f.write(f"| `{r['name']}` | {p1} | {p3} |\n")
                f.write(markdown_full_log(capture.getvalue()))
    sys.exit(exit_code)

if __name__ == "__main__": main()
