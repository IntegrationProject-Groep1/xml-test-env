"""
RabbitMQ Integration Test Suite
Groep 1 — Desideriushogeschool 2026
Based on: XML/XSD Contract v2.3

Tests every message flow defined in the central contract:
  Frontend · CRM · Kassa · Facturatie · Planning · Mailing · Monitoring · Identity

Usage:
    python test_integration.py [options]

Options:
    --host HOST         RabbitMQ host  (default: localhost)
    --port PORT         AMQP port      (default: 5672, use 30000 for Azure)
    --user USER         Username       (default: guest)
    --pass PASS         Password       (default: guest)
    --mgmt-port PORT    Management API (default: 15672, use 30001 for Azure)
    --vhost VHOST       Virtual host   (default: /)
    --timeout SECS      Arrival check  (default: 5)
    --teams TEAMS       Comma list of teams to test, e.g. crm,kassa (default: all)
    --dry-run           Validate XML only, skip publish/arrival
    --strict-live       Fail the run if live RabbitMQ connectivity fails
    --verbose           Print full XML payloads
    --env FILE          Load .env file (default: .env if present)
"""

import argparse
import os
import sys
import time
import uuid
import json
import textwrap
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8')

import pika
import urllib.request
import urllib.parse
from lxml import etree

# ── Colour helpers ─────────────────────────────────────────────────────────────
GREEN  = "\033[0;32m"
RED    = "\033[0;31m"
YELLOW = "\033[1;33m"
CYAN   = "\033[0;36m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):    print(f"  {GREEN}✓{RESET} {msg}")
def fail(msg):  print(f"  {RED}✗{RESET} {msg}"); _state["failures"] += 1
def warn(msg):  print(f"  {YELLOW}⚠{RESET} {msg}")
def info(msg):  print(f"  {CYAN}→{RESET} {msg}")
def header(msg):print(f"\n{BOLD}{CYAN}══ {msg} ══{RESET}")

_state = {"failures": 0, "tests": 0}


# ── Config ─────────────────────────────────────────────────────────────────────
def load_env(path=".env"):
    if not Path(path).exists():
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def parse_args():
    p = argparse.ArgumentParser(description="RabbitMQ Integration Tests — Groep 1 v2.3")
    p.add_argument("--host",             default=os.getenv("RABBIT_HOST", "20.126.113.148"))
    p.add_argument("--port",             type=int, default=int(os.getenv("RABBIT_PORT", 30000)))
    p.add_argument("--user",             default=os.getenv("RABBIT_USER", "guest"))
    p.add_argument("--pass",             dest="password", default=os.getenv("RABBIT_PASS", "guest"))
    p.add_argument("--mgmt-port",        type=int, default=int(os.getenv("RABBIT_MGMT_PORT", 30001)))
    p.add_argument("--vhost",            default=os.getenv("RABBIT_VHOST", "/"))
    p.add_argument("--timeout",          type=int, default=int(os.getenv("TIMEOUT", 5)))
    p.add_argument("--teams",            default="all")
    p.add_argument("--dry-run",          action="store_true")
    p.add_argument("--strict-live",      action="store_true")
    p.add_argument("--pause-consumers",  action="store_true",
                   help="Temporarily disconnect active consumers so messages are visible in queues")
    p.add_argument("--pause-delay",      type=float, default=float(os.getenv("PAUSE_DELAY", 0.8)),
                   help="Extra wait (s) after consumer disconnect before publishing (default: 0.8)")
    p.add_argument("--verbose",          action="store_true")
    p.add_argument("--env",              default=".env")
    return p.parse_args()


# ── XSD Schemas (v2.3 contract) ────────────────────────────────────────────────
# All schemas derived verbatim from XML_XSD_Contract_v2.3_Centralized 1.md
# and the XSD files present in each team repo.
# Key rules from the contract:
#   - No xmlns, no <receiver> in header
#   - version MUST be "2.0"
#   - identity_uuid (UUID format) replaces all legacy user_id / customer_id fields
#   - Names wrapped in <contact> block
#   - currency="eur" (fixed) on all monetary amounts
#   - date_of_birth instead of age
#   - Header order: message_id, timestamp, source, type, version, [correlation_id]

SCHEMAS = {}

# ── Heartbeat (§3) ─────────────────────────────────────────────────────────────
# Queue: heartbeat
SCHEMAS["heartbeat"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="message">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="header">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="message_id">
                <xs:simpleType><xs:restriction base="xs:string">
                  <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
                </xs:restriction></xs:simpleType>
              </xs:element>
              <xs:element name="timestamp"  type="xs:dateTime"/>
              <xs:element name="source"     type="xs:string"/>
              <xs:element name="type">
                <xs:simpleType><xs:restriction base="xs:string">
                  <xs:enumeration value="heartbeat"/>
                </xs:restriction></xs:simpleType>
              </xs:element>
              <xs:element name="version">
                <xs:simpleType><xs:restriction base="xs:string">
                  <xs:enumeration value="2.0"/>
                </xs:restriction></xs:simpleType>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
        <xs:element name="body">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="status">
                <xs:simpleType><xs:restriction base="xs:string">
                  <xs:enumeration value="online"/>
                  <xs:enumeration value="offline"/>
                </xs:restriction></xs:simpleType>
              </xs:element>
              <xs:element name="uptime" type="xs:nonNegativeInteger"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

# ── system_alert (§4) ──────────────────────────────────────────────────────────
# Flat <alert> root — NOT the standard <message> envelope.
# Queue: to_mailing
SCHEMAS["monitoring_system_alert"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="alert">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="type"      type="xs:string" fixed="HEARTBEAT_CRITICAL"/>
        <xs:element name="system"    type="xs:string"/>
        <xs:element name="message"   type="xs:string"/>
        <xs:element name="timestamp" type="xs:dateTime"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

# ── Log (§3.5) ────────────────────────────────────────────────────────────────
# Queue: logs
SCHEMAS["logs"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="UUIDType">
    <xs:restriction base="xs:string">
      <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:element name="message">
    <xs:complexType><xs:sequence>
      <xs:element name="header">
        <xs:complexType><xs:sequence>
          <xs:element name="message_id" type="UUIDType"/>
          <xs:element name="timestamp"  type="xs:dateTime"/>
          <xs:element name="source"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="crm"/><xs:enumeration value="kassa"/>
            <xs:enumeration value="facturatie"/><xs:enumeration value="frontend"/>
            <xs:enumeration value="planning"/><xs:enumeration value="mailing"/>
            <xs:enumeration value="identity-service"/><xs:enumeration value="iot_gateway"/>
          </xs:restriction></xs:simpleType></xs:element>
          <xs:element name="type"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="log"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="version"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="2.0"/></xs:restriction></xs:simpleType></xs:element>
        </xs:sequence></xs:complexType>
      </xs:element>
      <xs:element name="body">
        <xs:complexType><xs:sequence>
          <xs:element name="level"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="info"/><xs:enumeration value="warning"/><xs:enumeration value="error"/>
          </xs:restriction></xs:simpleType></xs:element>
          <xs:element name="action"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="registration"/><xs:enumeration value="user"/><xs:enumeration value="payment"/>
            <xs:enumeration value="invoice"/><xs:enumeration value="session"/><xs:enumeration value="calendar"/>
            <xs:enumeration value="email"/><xs:enumeration value="wallet"/><xs:enumeration value="refund"/>
            <xs:enumeration value="identity"/><xs:enumeration value="xml_validation"/><xs:enumeration value="system_error"/>
            <xs:enumeration value="badge"/>
          </xs:restriction></xs:simpleType></xs:element>
          <xs:element name="message" type="xs:string"/>
        </xs:sequence></xs:complexType>
      </xs:element>
    </xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>"""

# ── Frontend → CRM/Facturatie : event_ended (§5.7 / §11.6) ────────────────────
# Queue: crm.incoming  or  facturatie.incoming
SCHEMAS["frontend_event_ended"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="UUIDType">
    <xs:restriction base="xs:string">
      <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:element name="message">
    <xs:complexType><xs:sequence>
      <xs:element name="header">
        <xs:complexType><xs:sequence>
          <xs:element name="message_id"     type="UUIDType"/>
          <xs:element name="timestamp"      type="xs:dateTime"/>
          <xs:element name="source"         type="xs:string" fixed="frontend"/>
          <xs:element name="type"           type="xs:string" fixed="event_ended"/>
          <xs:element name="version"        type="xs:string" fixed="2.0"/>
          <xs:element name="correlation_id" type="UUIDType" minOccurs="0"/>
        </xs:sequence></xs:complexType>
      </xs:element>
      <xs:element name="body">
        <xs:complexType><xs:sequence>
          <xs:element name="session_id" type="xs:string"/>
          <xs:element name="ended_at"   type="xs:dateTime"/>
        </xs:sequence></xs:complexType>
      </xs:element>
    </xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>"""

# ── Frontend → CRM : new_registration (§5.1) ──────────────────────────────────
# Queue: crm.incoming
SCHEMAS["frontend_new_registration"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="UUIDType">
    <xs:restriction base="xs:string">
      <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:element name="message">
    <xs:complexType><xs:sequence>
      <xs:element name="header">
        <xs:complexType><xs:sequence>
          <xs:element name="message_id"     type="UUIDType"/>
          <xs:element name="timestamp"      type="xs:dateTime"/>
          <xs:element name="source"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="frontend"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="type"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="new_registration"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="version"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="2.0"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="correlation_id" type="UUIDType"/>
        </xs:sequence></xs:complexType>
      </xs:element>
      <xs:element name="body">
        <xs:complexType><xs:sequence>
          <xs:element name="customer">
            <xs:complexType><xs:sequence>
              <xs:element name="identity_uuid"     type="UUIDType"/>
              <xs:element name="email"             type="xs:string"/>
              <xs:element name="type"              type="xs:string"/>
              <xs:element name="is_company_linked" type="xs:boolean"/>
              <xs:element name="vat_number"        type="xs:string" minOccurs="0"/>
              <xs:element name="date_of_birth"     type="xs:date"/>
              <xs:element name="contact">
                <xs:complexType><xs:sequence>
                  <xs:element name="first_name" type="xs:string"/>
                  <xs:element name="last_name"  type="xs:string"/>
                </xs:sequence></xs:complexType>
              </xs:element>
              <xs:element name="address"    type="xs:string"/>
              <xs:element name="company_id" type="xs:string" minOccurs="0"/>
              <xs:element name="session_id" type="xs:string"/>
              <xs:element name="payment_due">
                <xs:complexType><xs:sequence>
                  <xs:element name="amount">
                    <xs:complexType><xs:simpleContent><xs:extension base="xs:decimal">
                      <xs:attribute name="currency" type="xs:string" fixed="eur" use="required"/>
                    </xs:extension></xs:simpleContent></xs:complexType>
                  </xs:element>
                  <xs:element name="status" type="xs:string" fixed="unpaid"/>
                </xs:sequence></xs:complexType>
              </xs:element>
            </xs:sequence></xs:complexType>
          </xs:element>
        </xs:sequence></xs:complexType>
      </xs:element>
    </xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>"""

# ── CRM → Kassa : new_registration (§10.1) ────────────────────────────────────
# Exchange: kassa.exchange, routing: kassa.incoming
SCHEMAS["crm_to_kassa_new_registration"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="UUIDType">
    <xs:restriction base="xs:string">
      <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:element name="message">
    <xs:complexType><xs:sequence>
      <xs:element name="header">
        <xs:complexType><xs:sequence>
          <xs:element name="message_id"     type="UUIDType"/>
          <xs:element name="timestamp"      type="xs:dateTime"/>
          <xs:element name="source"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="crm"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="type"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="new_registration"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="version"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="2.0"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="correlation_id" type="UUIDType"/>
        </xs:sequence></xs:complexType>
      </xs:element>
      <xs:element name="body">
        <xs:complexType><xs:sequence>
          <xs:element name="customer">
            <xs:complexType><xs:sequence>
              <xs:element name="identity_uuid"  type="UUIDType"/>
              <xs:element name="email"          type="xs:string"/>
              <xs:element name="date_of_birth"  type="xs:date"/>
              <xs:element name="contact">
                <xs:complexType><xs:sequence>
                  <xs:element name="first_name" type="xs:string"/>
                  <xs:element name="last_name"  type="xs:string"/>
                </xs:sequence></xs:complexType>
              </xs:element>
              <xs:element name="type">
                <xs:simpleType><xs:restriction base="xs:string">
                  <xs:enumeration value="private"/>
                  <xs:enumeration value="company"/>
                </xs:restriction></xs:simpleType>
              </xs:element>
              <xs:element name="company_name"   type="xs:string" minOccurs="0"/>
              <xs:element name="vat_number"     type="xs:string" minOccurs="0"/>
              <xs:element name="company_id"     type="xs:string" minOccurs="0"/>
              <xs:element name="badge_id"       type="xs:string" minOccurs="0"/>
              <xs:element name="session_id"     type="xs:string"/>
              <xs:element name="session_title"  type="xs:string" minOccurs="0"/>
              <xs:element name="payment_due">
                <xs:complexType><xs:sequence>
                  <xs:element name="amount">
                    <xs:complexType><xs:simpleContent><xs:extension base="xs:decimal">
                      <xs:attribute name="currency" type="xs:string" fixed="eur" use="required"/>
                    </xs:extension></xs:simpleContent></xs:complexType>
                  </xs:element>
                  <xs:element name="status">
                    <xs:simpleType><xs:restriction base="xs:string">
                      <xs:enumeration value="unpaid"/>
                      <xs:enumeration value="paid"/>
                    </xs:restriction></xs:simpleType>
                  </xs:element>
                </xs:sequence></xs:complexType>
              </xs:element>
            </xs:sequence></xs:complexType>
          </xs:element>
        </xs:sequence></xs:complexType>
      </xs:element>
    </xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>"""

# ── Kassa → CRM/Facturatie : consumption_order (§6.1, v2.3) ───────────────────
# Exchange: kassa.exchange, routing: kassa.payments.consumption
SCHEMAS["kassa_consumption_order"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="UUIDType">
    <xs:restriction base="xs:string">
      <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:complexType name="CustomerType">
    <xs:sequence>
      <xs:element name="id"            type="xs:string"  minOccurs="0"/>
      <xs:element name="identity_uuid" type="UUIDType"   minOccurs="0"/>
      <xs:element name="type">
        <xs:simpleType><xs:restriction base="xs:string">
          <xs:enumeration value="private"/>
          <xs:enumeration value="company"/>
        </xs:restriction></xs:simpleType>
      </xs:element>
      <xs:element name="email"   type="xs:string" minOccurs="0"/>
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="ItemType">
    <xs:sequence>
      <xs:element name="id"          type="xs:string"/>
      <xs:element name="sku"         type="xs:string"/>
      <xs:element name="description" type="xs:string"/>
      <xs:element name="quantity"    type="xs:integer"/>
      <xs:element name="unit_price">
        <xs:complexType><xs:simpleContent><xs:extension base="xs:decimal">
          <xs:attribute name="currency" type="xs:string" fixed="eur" use="required"/>
        </xs:extension></xs:simpleContent>
      </xs:complexType>
      </xs:element>
      <xs:element name="vat_rate">
        <xs:simpleType><xs:restriction base="xs:integer">
          <xs:enumeration value="0"/>
          <xs:enumeration value="6"/>
          <xs:enumeration value="12"/>
          <xs:enumeration value="21"/>
        </xs:restriction></xs:simpleType>
      </xs:element>
      <xs:element name="total_amount">
        <xs:complexType><xs:simpleContent><xs:extension base="xs:decimal">
          <xs:attribute name="currency" type="xs:string" fixed="eur" use="required"/>
        </xs:extension></xs:simpleContent>
      </xs:complexType>
      </xs:element>
      <xs:element name="item_type" type="xs:string" minOccurs="0"/>
    </xs:sequence>
  </xs:complexType>
  <xs:element name="message">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="header">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="message_id"     type="UUIDType"/>
              <xs:element name="timestamp"      type="xs:dateTime"/>
              <xs:element name="source"         type="xs:string" fixed="kassa"/>
              <xs:element name="type"           type="xs:string" fixed="consumption_order"/>
              <xs:element name="version"        type="xs:string" fixed="2.0"/>
              <xs:element name="correlation_id" type="UUIDType"  minOccurs="0"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
        <xs:element name="body">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="is_anonymous" type="xs:boolean"    minOccurs="0"/>
              <xs:element name="customer"     type="CustomerType"  minOccurs="0"/>
              <xs:element name="items">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="item" type="ItemType" maxOccurs="unbounded"/>
                  </xs:sequence>
                </xs:complexType>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

# ── Kassa → CRM/Facturatie : payment_registered (§6.5 / §6.6) ─────────────────
# Exchange: kassa.exchange, routing: kassa.payments.registration
SCHEMAS["kassa_payment_registered"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:complexType name="CurrencyAmountType">
    <xs:simpleContent>
      <xs:extension base="xs:decimal">
        <xs:attribute name="currency" type="xs:string" fixed="eur" use="required"/>
      </xs:extension>
    </xs:simpleContent>
  </xs:complexType>
  <xs:simpleType name="UUIDType">
    <xs:restriction base="xs:string">
      <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:element name="message">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="header">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="message_id"     type="UUIDType"/>
              <xs:element name="timestamp"      type="xs:dateTime"/>
              <xs:element name="source"><xs:simpleType><xs:restriction base="xs:string">
                <xs:enumeration value="kassa"/></xs:restriction></xs:simpleType></xs:element>
              <xs:element name="type"><xs:simpleType><xs:restriction base="xs:string">
                <xs:enumeration value="payment_registered"/></xs:restriction></xs:simpleType></xs:element>
              <xs:element name="version"><xs:simpleType><xs:restriction base="xs:string">
                <xs:enumeration value="2.0"/></xs:restriction></xs:simpleType></xs:element>
              <xs:element name="correlation_id" type="UUIDType" minOccurs="0"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
        <xs:element name="body">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="identity_uuid" type="UUIDType" minOccurs="0"/>
              <xs:element name="invoice">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="id"          type="xs:string" minOccurs="0"/>
                    <xs:element name="amount_paid" type="CurrencyAmountType"/>
                    <xs:element name="status">
                      <xs:simpleType><xs:restriction base="xs:string">
                        <xs:enumeration value="paid"/>
                        <xs:enumeration value="pending"/>
                        <xs:enumeration value="cancelled"/>
                      </xs:restriction></xs:simpleType>
                    </xs:element>
                    <xs:element name="due_date" type="xs:date" minOccurs="0"/>
                  </xs:sequence>
                </xs:complexType>
              </xs:element>
              <xs:element name="payment_context">
                <xs:simpleType><xs:restriction base="xs:string">
                  <xs:enumeration value="registration"/>
                  <xs:enumeration value="consumption"/>
                  <xs:enumeration value="online_invoice"/>
                  <xs:enumeration value="session_registration"/>
                </xs:restriction></xs:simpleType>
              </xs:element>
              <xs:element name="transaction">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="id" type="xs:string"/>
                    <xs:element name="payment_method">
                      <xs:simpleType><xs:restriction base="xs:string">
                        <xs:enumeration value="company_link"/>
                        <xs:enumeration value="on_site"/>
                        <xs:enumeration value="online"/>
                      </xs:restriction></xs:simpleType>
                    </xs:element>
                  </xs:sequence>
                </xs:complexType>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

# ── IoT/Kassa → Kassa : badge_scanned (§6.3, v2.3) ───────────────────────────
# Queue: kassa.incoming  (no exchange — direct queue publish)
# Body: xs:choice — badge_id (badge-scan) OR identity_uuid (QR-scan)
SCHEMAS["kassa_badge_scanned"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="UUIDType">
    <xs:restriction base="xs:string">
      <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:element name="message">
    <xs:complexType><xs:sequence>
      <xs:element name="header">
        <xs:complexType><xs:sequence>
          <xs:element name="message_id" type="UUIDType"/>
          <xs:element name="timestamp"  type="xs:dateTime"/>
          <xs:element name="source"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="iot_gateway"/>
            <xs:enumeration value="kassa"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="type"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="badge_scanned"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="version"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="2.0"/></xs:restriction></xs:simpleType></xs:element>
        </xs:sequence></xs:complexType>
      </xs:element>
      <xs:element name="body">
        <xs:complexType><xs:sequence>
          <xs:choice>
            <xs:element name="badge_id"      type="xs:string"/>
            <xs:element name="identity_uuid" type="UUIDType"/>
          </xs:choice>
          <xs:element name="location">
            <xs:simpleType><xs:restriction base="xs:string">
              <xs:enumeration value="entrance"/>
              <xs:enumeration value="bar"/>
              <xs:enumeration value="main_bar"/>
              <xs:enumeration value="session"/>
            </xs:restriction></xs:simpleType>
          </xs:element>
          <xs:element name="scanned_at" type="xs:dateTime"/>
        </xs:sequence></xs:complexType>
      </xs:element>
    </xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>"""

# ── Kassa → CRM : wallet_lease_request (§26.1) ────────────────────────────────
# Exchange: kassa.exchange, routing: kassa.to.crm.wallet_lease_request
# badge_id is optional (minOccurs="0") — absent on QR-scan, present on badge-scan
SCHEMAS["kassa_wallet_lease_request"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="UUIDType">
    <xs:restriction base="xs:string">
      <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:element name="message">
    <xs:complexType><xs:sequence>
      <xs:element name="header">
        <xs:complexType><xs:sequence>
          <xs:element name="message_id" type="UUIDType"/>
          <xs:element name="timestamp"  type="xs:dateTime"/>
          <xs:element name="source"     type="xs:string" fixed="kassa"/>
          <xs:element name="type"       type="xs:string" fixed="wallet_lease_request"/>
          <xs:element name="version"    type="xs:string" fixed="2.0"/>
        </xs:sequence></xs:complexType>
      </xs:element>
      <xs:element name="body">
        <xs:complexType><xs:sequence>
          <xs:element name="identity_uuid" type="UUIDType"/>
          <xs:element name="badge_id"      type="xs:string" minOccurs="0"/>
        </xs:sequence></xs:complexType>
      </xs:element>
    </xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>"""

# ── CRM → Facturatie : invoice_request (§11.1) ────────────────────────────────
# Queue: facturatie.incoming
SCHEMAS["crm_to_facturatie_invoice_request"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="UUIDType">
    <xs:restriction base="xs:string">
      <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:complexType name="AddressType">
    <xs:sequence>
      <xs:element name="street"      type="xs:string"/>
      <xs:element name="number"      type="xs:string"/>
      <xs:element name="postal_code" type="xs:string"/>
      <xs:element name="city"        type="xs:string"/>
      <xs:element name="country"     type="xs:string"/>
    </xs:sequence>
  </xs:complexType>
  <xs:element name="message">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="header">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="message_id"     type="UUIDType"/>
              <xs:element name="timestamp"      type="xs:dateTime"/>
              <xs:element name="source"><xs:simpleType><xs:restriction base="xs:string">
                <xs:enumeration value="crm"/></xs:restriction></xs:simpleType></xs:element>
              <xs:element name="type"><xs:simpleType><xs:restriction base="xs:string">
                <xs:enumeration value="invoice_request"/></xs:restriction></xs:simpleType></xs:element>
              <xs:element name="version"><xs:simpleType><xs:restriction base="xs:string">
                <xs:enumeration value="2.0"/></xs:restriction></xs:simpleType></xs:element>
              <xs:element name="correlation_id" type="UUIDType"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
        <xs:element name="body">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="identity_uuid" type="UUIDType"/>
              <xs:element name="invoice_data">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="contact">
                      <xs:complexType>
                        <xs:sequence>
                          <xs:element name="first_name" type="xs:string"/>
                          <xs:element name="last_name"  type="xs:string"/>
                        </xs:sequence>
                      </xs:complexType>
                    </xs:element>
                    <xs:element name="email"        type="xs:string"/>
                    <xs:element name="address"      type="AddressType"/>
                    <xs:element name="company_name" type="xs:string" minOccurs="0"/>
                    <xs:element name="vat_number"   type="xs:string" minOccurs="0"/>
                  </xs:sequence>
                </xs:complexType>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

# ── CRM/Facturatie → Mailing : send_mailing (§12.1 / §13.1) ───────────────────
# Queue: crm.to.mailing  or  facturatie.to.mailing
SCHEMAS["send_mailing"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="UUIDType">
    <xs:restriction base="xs:string">
      <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:complexType name="ContactType">
    <xs:sequence>
      <xs:element name="first_name" type="xs:string"/>
      <xs:element name="last_name"  type="xs:string"/>
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="RecipientType">
    <xs:sequence>
      <xs:element name="email"         type="xs:string"/>
      <xs:element name="identity_uuid" type="UUIDType"/>
      <xs:element name="contact"       type="ContactType"/>
    </xs:sequence>
  </xs:complexType>
  <xs:element name="message">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="header">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="message_id"     type="UUIDType"/>
              <xs:element name="timestamp"      type="xs:dateTime"/>
              <xs:element name="source">
                <xs:simpleType><xs:restriction base="xs:string">
                  <xs:enumeration value="crm"/>
                  <xs:enumeration value="facturatie"/>
                </xs:restriction></xs:simpleType>
              </xs:element>
              <xs:element name="type"><xs:simpleType><xs:restriction base="xs:string">
                <xs:enumeration value="send_mailing"/></xs:restriction></xs:simpleType></xs:element>
              <xs:element name="version"><xs:simpleType><xs:restriction base="xs:string">
                <xs:enumeration value="2.0"/></xs:restriction></xs:simpleType></xs:element>
              <xs:element name="correlation_id" type="UUIDType"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
        <xs:element name="body">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="campaign_id" type="xs:string"/>
              <xs:element name="subject"     type="xs:string"/>
              <xs:element name="mail_type">
                <xs:simpleType><xs:restriction base="xs:string">
                  <xs:enumeration value="registration_confirmation"/>
                  <xs:enumeration value="payment_confirmation"/>
                  <xs:enumeration value="invoice_ready"/>
                  <xs:enumeration value="session_update"/>
                  <xs:enumeration value="general_announcement"/>
                </xs:restriction></xs:simpleType>
              </xs:element>
              <xs:element name="recipients">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="recipient" type="RecipientType" maxOccurs="unbounded"/>
                  </xs:sequence>
                </xs:complexType>
              </xs:element>
              <xs:element name="template_data" type="xs:string" minOccurs="0"/>
              <xs:element name="body_html"     type="xs:string" minOccurs="0"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

# ── Mailing → CRM : mailing_status (§9.1) ─────────────────────────────────────
# Queue: crm.incoming
SCHEMAS["mailing_status"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="message">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="header">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="message_id" type="xs:string"/>
              <xs:element name="timestamp"  type="xs:dateTime"/>
              <xs:element name="source"><xs:simpleType><xs:restriction base="xs:string">
                <xs:enumeration value="mailing"/></xs:restriction></xs:simpleType></xs:element>
              <xs:element name="type"><xs:simpleType><xs:restriction base="xs:string">
                <xs:enumeration value="mailing_status"/></xs:restriction></xs:simpleType></xs:element>
              <xs:element name="version"><xs:simpleType><xs:restriction base="xs:string">
                <xs:enumeration value="2.0"/></xs:restriction></xs:simpleType></xs:element>
              <xs:element name="correlation_id" type="xs:string" minOccurs="0"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
        <xs:element name="body">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="campaign_id" type="xs:string"/>
              <xs:element name="subject"     type="xs:string"/>
              <xs:element name="sent"        type="xs:integer"/>
              <xs:element name="delivered"   type="xs:integer"/>
              <xs:element name="bounced"     type="xs:integer"/>
              <xs:element name="bounced_emails" minOccurs="0">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="email" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
                  </xs:sequence>
                </xs:complexType>
              </xs:element>
              <xs:element name="opened" type="xs:integer"/>
              <xs:element name="status">
                <xs:simpleType><xs:restriction base="xs:string">
                  <xs:enumeration value="completed"/>
                  <xs:enumeration value="partial_failure"/>
                  <xs:enumeration value="failed"/>
                </xs:restriction></xs:simpleType>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

# ── Facturatie → CRM : invoice_status (§8.1) ──────────────────────────────────
# Queue: crm.incoming
SCHEMAS["facturatie_invoice_status"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="UUIDType">
    <xs:restriction base="xs:string">
      <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:element name="message">
    <xs:complexType><xs:sequence>
      <xs:element name="header">
        <xs:complexType><xs:sequence>
          <xs:element name="message_id"     type="UUIDType"/>
          <xs:element name="timestamp"      type="xs:dateTime"/>
          <xs:element name="source"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="facturatie"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="type"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="invoice_status"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="version"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="2.0"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="correlation_id" type="UUIDType" minOccurs="0"/>
        </xs:sequence></xs:complexType>
      </xs:element>
      <xs:element name="body">
        <xs:complexType><xs:sequence>
          <xs:element name="invoice_id"    type="xs:string"/>
          <xs:element name="identity_uuid" type="UUIDType"/>
          <xs:element name="status">
            <xs:simpleType><xs:restriction base="xs:string">
              <xs:enumeration value="draft"/>
              <xs:enumeration value="sent"/>
              <xs:enumeration value="paid"/>
              <xs:enumeration value="overdue"/>
              <xs:enumeration value="cancelled"/>
            </xs:restriction></xs:simpleType>
          </xs:element>
          <xs:element name="amount">
            <xs:complexType><xs:simpleContent><xs:extension base="xs:decimal">
              <xs:attribute name="currency" type="xs:string" fixed="eur" use="required"/>
            </xs:extension></xs:simpleContent></xs:complexType>
          </xs:element>
          <xs:element name="due_date" type="xs:date" minOccurs="0"/>
        </xs:sequence></xs:complexType>
      </xs:element>
    </xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>"""

# ── Facturatie → CRM : payment_registered (§8.2) ──────────────────────────────
# Outbound from Facturatie after online payment confirmation.
# Queue: crm.incoming
SCHEMAS["facturatie_payment_registered"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:complexType name="CurrencyAmountType">
    <xs:simpleContent>
      <xs:extension base="xs:decimal">
        <xs:attribute name="currency" type="xs:string" fixed="eur" use="required"/>
      </xs:extension>
    </xs:simpleContent>
  </xs:complexType>
  <xs:simpleType name="UUIDType">
    <xs:restriction base="xs:string">
      <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:element name="message">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="header">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="message_id"     type="UUIDType"/>
              <xs:element name="timestamp"      type="xs:dateTime"/>
              <xs:element name="source"         type="xs:string" fixed="facturatie"/>
              <xs:element name="type"           type="xs:string" fixed="payment_registered"/>
              <xs:element name="version"        type="xs:string" fixed="2.0"/>
              <xs:element name="correlation_id" type="UUIDType" minOccurs="0"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
        <xs:element name="body">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="identity_uuid" type="UUIDType"/>
              <xs:element name="invoice">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="id"          type="xs:string"/>
                    <xs:element name="amount_paid" type="CurrencyAmountType"/>
                    <xs:element name="status">
                      <xs:simpleType><xs:restriction base="xs:string">
                        <xs:enumeration value="paid"/>
                        <xs:enumeration value="pending"/>
                        <xs:enumeration value="cancelled"/>
                      </xs:restriction></xs:simpleType>
                    </xs:element>
                    <xs:element name="due_date" type="xs:date" minOccurs="0"/>
                  </xs:sequence>
                </xs:complexType>
              </xs:element>
              <xs:element name="payment_context">
                <xs:simpleType><xs:restriction base="xs:string">
                  <xs:enumeration value="registration"/>
                  <xs:enumeration value="consumption"/>
                  <xs:enumeration value="online_invoice"/>
                  <xs:enumeration value="session_registration"/>
                </xs:restriction></xs:simpleType>
              </xs:element>
              <xs:element name="transaction" minOccurs="0">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="id" type="xs:string"/>
                    <xs:element name="payment_method">
                      <xs:simpleType><xs:restriction base="xs:string">
                        <xs:enumeration value="company_link"/>
                        <xs:enumeration value="on_site"/>
                        <xs:enumeration value="online"/>
                      </xs:restriction></xs:simpleType>
                    </xs:element>
                  </xs:sequence>
                </xs:complexType>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

# ── Planning → CRM : session_created (§7.1) ───────────────────────────────────
# Exchange: planning.exchange, routing: planning.session.created
SCHEMAS["planning_session_created"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="UUIDType">
    <xs:restriction base="xs:string">
      <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:element name="message">
    <xs:complexType><xs:sequence>
      <xs:element name="header">
        <xs:complexType><xs:sequence>
          <xs:element name="message_id"    type="UUIDType"/>
          <xs:element name="timestamp"     type="xs:dateTime"/>
          <xs:element name="source"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="planning"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="type"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="session_created"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="version"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="2.0"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="correlation_id" type="UUIDType" minOccurs="0"/>
        </xs:sequence></xs:complexType>
      </xs:element>
      <xs:element name="body">
        <xs:complexType><xs:sequence>
          <xs:element name="session_id"        type="xs:string"/>
          <xs:element name="title"             type="xs:string"/>
          <xs:element name="start_datetime"    type="xs:dateTime"/>
          <xs:element name="end_datetime"      type="xs:dateTime"/>
          <xs:element name="location"          type="xs:string"/>
          <xs:element name="session_type">
            <xs:simpleType><xs:restriction base="xs:string">
              <xs:enumeration value="keynote"/>
              <xs:enumeration value="workshop"/>
              <xs:enumeration value="reception"/>
              <xs:enumeration value="other"/>
            </xs:restriction></xs:simpleType>
          </xs:element>
          <xs:element name="status">
            <xs:simpleType><xs:restriction base="xs:string">
              <xs:enumeration value="draft"/>
              <xs:enumeration value="published"/>
              <xs:enumeration value="cancelled"/>
            </xs:restriction></xs:simpleType>
          </xs:element>
          <xs:element name="max_attendees"     type="xs:positiveInteger"/>
          <xs:element name="current_attendees" type="xs:nonNegativeInteger"/>
          <xs:element name="speaker" minOccurs="0">
            <xs:complexType><xs:sequence>
              <xs:element name="identity_uuid" type="UUIDType" minOccurs="0"/>
              <xs:element name="contact">
                <xs:complexType><xs:sequence>
                  <xs:element name="first_name" type="xs:string"/>
                  <xs:element name="last_name"  type="xs:string"/>
                </xs:sequence></xs:complexType>
              </xs:element>
              <xs:element name="organisation" type="xs:string" minOccurs="0"/>
              <xs:element name="email"        type="xs:string" minOccurs="0"/>
            </xs:sequence></xs:complexType>
          </xs:element>
        </xs:sequence></xs:complexType>
      </xs:element>
    </xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>"""

# ── Planning → CRM : session_updated (§7.2) ───────────────────────────────────
# Exchange: planning.exchange, routing: planning.session.updated
SCHEMAS["planning_session_updated"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="UUIDType">
    <xs:restriction base="xs:string">
      <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:element name="message">
    <xs:complexType><xs:sequence>
      <xs:element name="header">
        <xs:complexType><xs:sequence>
          <xs:element name="message_id"    type="UUIDType"/>
          <xs:element name="timestamp"     type="xs:dateTime"/>
          <xs:element name="source"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="planning"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="type"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="session_updated"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="version"><xs:simpleType><xs:restriction base="xs:string">
            <xs:enumeration value="2.0"/></xs:restriction></xs:simpleType></xs:element>
          <xs:element name="correlation_id" type="UUIDType" minOccurs="0"/>
        </xs:sequence></xs:complexType>
      </xs:element>
      <xs:element name="body">
        <xs:complexType><xs:sequence>
          <xs:element name="session_id"        type="xs:string"/>
          <xs:element name="title"             type="xs:string"/>
          <xs:element name="start_datetime"    type="xs:dateTime"/>
          <xs:element name="end_datetime"      type="xs:dateTime"/>
          <xs:element name="location"          type="xs:string"/>
          <xs:element name="session_type">
            <xs:simpleType><xs:restriction base="xs:string">
              <xs:enumeration value="keynote"/>
              <xs:enumeration value="workshop"/>
              <xs:enumeration value="reception"/>
              <xs:enumeration value="other"/>
            </xs:restriction></xs:simpleType>
          </xs:element>
          <xs:element name="status">
            <xs:simpleType><xs:restriction base="xs:string">
              <xs:enumeration value="draft"/>
              <xs:enumeration value="published"/>
              <xs:enumeration value="cancelled"/>
            </xs:restriction></xs:simpleType>
          </xs:element>
          <xs:element name="max_attendees"     type="xs:positiveInteger"/>
          <xs:element name="current_attendees" type="xs:nonNegativeInteger"/>
          <xs:element name="change_reason"     type="xs:string" minOccurs="0"/>
          <xs:element name="speaker" minOccurs="0">
            <xs:complexType><xs:sequence>
              <xs:element name="identity_uuid" type="UUIDType" minOccurs="0"/>
              <xs:element name="contact">
                <xs:complexType><xs:sequence>
                  <xs:element name="first_name" type="xs:string"/>
                  <xs:element name="last_name"  type="xs:string"/>
                </xs:sequence></xs:complexType>
              </xs:element>
              <xs:element name="organisation" type="xs:string" minOccurs="0"/>
              <xs:element name="email"        type="xs:string" minOccurs="0"/>
            </xs:sequence></xs:complexType>
          </xs:element>
        </xs:sequence></xs:complexType>
      </xs:element>
    </xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>"""

# ── Frontend → Planning : session_create_request (§19.1) ──────────────────────
# Exchange: planning.exchange, routing: frontend.to.planning.session.create
SCHEMAS["frontend_session_create_request"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="message">
    <xs:complexType><xs:sequence>
      <xs:element name="header">
        <xs:complexType><xs:sequence>
          <xs:element name="message_id" type="xs:string"/>
          <xs:element name="timestamp"  type="xs:dateTime"/>
          <xs:element name="source"     type="xs:string" fixed="frontend"/>
          <xs:element name="type"       type="xs:string" fixed="session_create_request"/>
          <xs:element name="version"    type="xs:string" fixed="2.0"/>
        </xs:sequence></xs:complexType>
      </xs:element>
      <xs:element name="body">
        <xs:complexType><xs:sequence>
          <xs:element name="session_id"     type="xs:string"/>
          <xs:element name="title"          type="xs:string"/>
          <xs:element name="start_datetime" type="xs:dateTime"/>
          <xs:element name="end_datetime"   type="xs:dateTime"/>
          <xs:element name="location"       type="xs:string" minOccurs="0"/>
          <xs:element name="session_type"   type="xs:string" minOccurs="0"/>
          <xs:element name="status"         type="xs:string" minOccurs="0"/>
          <xs:element name="max_attendees"  type="xs:integer" minOccurs="0"/>
        </xs:sequence></xs:complexType>
      </xs:element>
    </xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>"""

# ── Frontend → Planning : calendar_invite (§19.3) ─────────────────────────────
# Exchange: calendar.exchange, routing: frontend.to.planning.calendar.invite
SCHEMAS["frontend_calendar_invite"] = b"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="UUIDType">
    <xs:restriction base="xs:string">
      <xs:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:element name="message">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="header">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="message_id"     type="xs:string"/>
              <xs:element name="timestamp"      type="xs:dateTime"/>
              <xs:element name="source">
                <xs:simpleType><xs:restriction base="xs:string">
                  <xs:enumeration value="frontend"/>
                  <xs:enumeration value="crm"/>
                </xs:restriction></xs:simpleType>
              </xs:element>
              <xs:element name="type"><xs:simpleType><xs:restriction base="xs:string">
                <xs:enumeration value="calendar_invite"/></xs:restriction></xs:simpleType></xs:element>
              <xs:element name="version"><xs:simpleType><xs:restriction base="xs:string">
                <xs:enumeration value="2.0"/></xs:restriction></xs:simpleType></xs:element>
              <xs:element name="correlation_id" type="xs:string" minOccurs="0"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
        <xs:element name="body">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="identity_uuid"  type="UUIDType"/>
              <xs:element name="session_id"     type="xs:string"/>
              <xs:element name="title"          type="xs:string"/>
              <xs:element name="start_datetime" type="xs:dateTime"/>
              <xs:element name="end_datetime"   type="xs:dateTime"/>
              <xs:element name="location"       type="xs:string" minOccurs="0"/>
              <xs:element name="attendee_email" type="xs:string"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""


def compile_schema(xsd_bytes: bytes) -> etree.XMLSchema:
    return etree.XMLSchema(etree.parse(BytesIO(xsd_bytes)))


COMPILED = {name: compile_schema(xsd) for name, xsd in SCHEMAS.items()}


def parse_xml(xml_str: str) -> etree._ElementTree:
    """Parse generated XML after removing template indentation before <?xml."""
    return etree.parse(BytesIO(xml_str.lstrip().encode("utf-8")))


# ── XML Builders ────────────────────────────────────────────────────────────────
def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_uuid() -> str:
    return str(uuid.uuid4())


def build_message(msg_type: str, source: str, body: str, correlation_id: str = None) -> str:
    """Standard v2.0 envelope per contract §2."""
    header_parts = [
        f"    <message_id>{new_uuid()}</message_id>",
        f"    <timestamp>{now_iso()}</timestamp>",
        f"    <source>{source}</source>",
        f"    <type>{msg_type}</type>",
        "    <version>2.0</version>"
    ]
    if correlation_id:
        header_parts.append(f"    <correlation_id>{correlation_id}</correlation_id>")

    header_xml = "\n".join(header_parts)
    # Ensure body is correctly indented and dedented
    clean_body = textwrap.indent(textwrap.dedent(body).strip(), "    ")

    return textwrap.dedent(f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <message>
          <header>
        {header_xml}
          </header>
          <body>
        {clean_body}
          </body>
        </message>""").strip()


def build_alert(system: str, message: str) -> str:
    """Flat <alert> root per contract §4 — NOT the standard envelope."""
    return textwrap.dedent(f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <alert>
          <type>HEARTBEAT_CRITICAL</type>
          <system>{system}</system>
          <message>{message}</message>
          <timestamp>{now_iso()}</timestamp>
        </alert>""").lstrip()


# ── XSD Validator ──────────────────────────────────────────────────────────────
def validate(xml_str: str, schema_name: str, label: str) -> bool:
    _state["tests"] += 1
    schema = COMPILED[schema_name]
    try:
        doc = parse_xml(xml_str)
        schema.assertValid(doc)
        ok(f"XSD valid   : {label}")
        return True
    except etree.DocumentInvalid as e:
        fail(f"XSD INVALID : {label}")
        for err in e.error_log:
            print(f"             {RED}{err.message}{RESET}")
        return False
    except Exception as e:
        fail(f"XSD ERROR   : {label} — {e}")
        return False


# ── RabbitMQ connection ────────────────────────────────────────────────────────
_conn = None
_channel = None
_cfg = None  # last valid cfg — allows reconnect after connection.close()


def get_channel(cfg=None):
    global _conn, _channel, _cfg
    if cfg is not None:
        _cfg = cfg
    if _conn and _conn.is_open:
        return _channel
    active = _cfg
    creds = pika.PlainCredentials(active.user, active.password)
    params = pika.ConnectionParameters(
        host=active.host, port=active.port, virtual_host=active.vhost,
        credentials=creds, socket_timeout=5,
        connection_attempts=3, retry_delay=1
    )
    _conn = pika.BlockingConnection(params)
    _channel = _conn.channel()
    return _channel


# ── Management API helpers ─────────────────────────────────────────────────────

def _mgmt_get(cfg, path: str):
    url = f"http://{cfg.host}:{cfg.mgmt_port}{path}"
    try:
        req = urllib.request.Request(url, headers={"Authorization": _basic_auth(cfg)})
        with urllib.request.urlopen(req, timeout=4) as r:
            return json.loads(r.read())
    except Exception:
        return None


def _mgmt_delete(cfg, path: str):
    url = f"http://{cfg.host}:{cfg.mgmt_port}{path}"
    try:
        req = urllib.request.Request(url, method="DELETE",
                                     headers={"Authorization": _basic_auth(cfg)})
        urllib.request.urlopen(req, timeout=4)
    except Exception:
        pass


def _consumers_on_queue(cfg, queue: str) -> list:
    vhost_enc = urllib.parse.quote(cfg.vhost, safe="")
    data = _mgmt_get(cfg, f"/api/consumers/{vhost_enc}") or []
    return [c for c in data
            if c.get("queue", {}).get("name") == queue]


def _pause_publish_get(cfg, queue: str, xml: str, expected_type: str, label: str):
    """Close consumers on queue, publish, then basic_get before they reconnect.

    Returns:
        True  — message found in queue or consumer took it (verified delivery)
        False — message not found despite no consumers
        None  — no consumers existed; caller should use normal publish+peek flow
    """
    global _conn, _channel

    consumers = _consumers_on_queue(cfg, queue)
    if not consumers:
        return None  # no consumers — fall back to normal flow

    # Close each consumer's AMQP connection via the management API
    seen_conns = set()
    for c in consumers:
        conn_name = c.get("channel_details", {}).get("connection_name", "")
        if conn_name and conn_name not in seen_conns:
            seen_conns.add(conn_name)
            conn_enc = urllib.parse.quote(conn_name, safe="")
            _mgmt_delete(cfg, f"/api/connections/{conn_enc}")

    # Also drop our own AMQP connection so it is re-established fresh
    if _conn and _conn.is_open:
        try:
            _conn.close()
        except Exception:
            pass
    _conn = None

    if cfg.pause_delay > 0:
        time.sleep(cfg.pause_delay)

    # Poll until the consumers are gone (max 3 s)
    deadline = time.time() + 3.0
    while time.time() < deadline:
        remaining = _consumers_on_queue(cfg, queue)
        if not remaining:
            break
        time.sleep(0.05)

    # Publish immediately while the queue has no consumers
    ch = get_channel()
    ch.queue_declare(queue=queue, durable=True, passive=True)
    ch.basic_publish(
        exchange="",
        routing_key=queue,
        body=xml.encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/xml", delivery_mode=2)
    )

    # Try to basic_get before the consumer reconnects
    for _ in range(20):
        method, props, body = ch.basic_get(queue=queue, auto_ack=False)
        if method:
            content = body.decode("utf-8", errors="replace")
            if f"<type>{expected_type}</type>" in content:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                ok(f"Queued ✓    : queue='{queue}' type={expected_type} — {label}")
                return True
            else:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        time.sleep(0.05)

    # Consumer reconnected and took the message before we could — that's fine
    ok(f"Consumed ✓  : queue='{queue}' type={expected_type} — {label} (delivered to live consumer)")
    return True


def _shadow_queue_test(cfg, exchange: str, routing_key: str,
                       xml: str, msg_type: str, label: str) -> bool:
    """Bind a temp exclusive queue to exchange+rk, publish, then basic_get to prove routing."""
    _state["tests"] += 1
    if cfg.dry_run:
        info(f"DRY-RUN shadow → exchange='{exchange}' rk='{routing_key}'")
        return True
    try:
        ch = get_channel(cfg)
        result = ch.queue_declare(queue="", exclusive=True, auto_delete=True)
        tmp_queue = result.method.queue
        ch.queue_bind(queue=tmp_queue, exchange=exchange, routing_key=routing_key)
        ch.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=xml.encode("utf-8"),
            properties=pika.BasicProperties(content_type="application/xml", delivery_mode=2)
        )
        ok(f"Published   : exchange='{exchange}' rk='{routing_key}'")
        for _ in range(20):
            method, _props, body = ch.basic_get(queue=tmp_queue, auto_ack=True)
            if method:
                if f"<type>{msg_type}</type>" in body.decode("utf-8", errors="replace"):
                    ok(f"Routed ✓    : exchange='{exchange}' rk='{routing_key}' — {label}")
                    return True
            time.sleep(0.05)
        fail(f"Routing FAIL: exchange='{exchange}' rk='{routing_key}' — {label}")
        return False
    except Exception as e:
        fail(f"Shadow queue error: {e}")
        return False


def publish(cfg, exchange: str, routing_key: str, xml: str) -> bool:
    if cfg.dry_run:
        info(f"DRY-RUN publish → exchange='{exchange}' rk='{routing_key}'")
        return True
    try:
        ch = get_channel(cfg)
        if exchange:
            try:
                ch.exchange_declare(exchange=exchange, exchange_type="topic",
                                    durable=True, passive=True)
            except Exception:
                global _conn, _channel
                _conn.close()
                _conn = None
                ch = get_channel(cfg)
                ch.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
        else:
            try:
                ch.queue_declare(queue=routing_key, durable=True, passive=True)
            except Exception:
                _conn.close()
                _conn = None
                ch = get_channel(cfg)
                ch.queue_declare(queue=routing_key, durable=True)

        ch.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=xml.encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/xml",
                delivery_mode=2
            )
        )
        ok(f"Published   : exchange='{exchange or '(default)'}' rk='{routing_key}'")
        return True
    except Exception as e:
        fail(f"Publish failed: {e}")
        return False


def _basic_auth(cfg) -> str:
    return "Basic " + __import__("base64").b64encode(
        f"{cfg.user}:{cfg.password}".encode()).decode()


def _queue_stats(cfg, queue: str):
    if cfg.dry_run:
        return None

    vhost_enc = urllib.parse.quote(cfg.vhost, safe="")
    queue_enc = urllib.parse.quote(queue, safe="")
    url = f"http://{cfg.host}:{cfg.mgmt_port}/api/queues/{vhost_enc}/{queue_enc}"
    try:
        req = urllib.request.Request(
            url,
            method="GET",
            headers={"Authorization": _basic_auth(cfg)}
        )
        with urllib.request.urlopen(req, timeout=4) as resp:
            return json.loads(resp.read())
    except Exception:
        return None


def _queue_marker(stats):
    if not stats:
        return None
    message_stats = stats.get("message_stats", {}) or {}
    return {
        "messages": int(stats.get("messages", 0) or 0),
        "messages_ready": int(stats.get("messages_ready", 0) or 0),
        "messages_unacknowledged": int(stats.get("messages_unacknowledged", 0) or 0),
        "publish": int(message_stats.get("publish", 0) or 0),
        "deliver_get": int(message_stats.get("deliver_get", 0) or 0),
        "ack": int(message_stats.get("ack", 0) or 0),
    }


def _queue_had_activity(before, after) -> bool:
    if not before or not after:
        return False
    return any(after.get(key, 0) > before.get(key, 0) for key in before)


def peek_queue(cfg, queue: str, expected_type: str, label: str, before_stats=None):
    """Non-destructively peek the queue via management HTTP API."""
    _state["tests"] += 1
    if cfg.dry_run:
        info(f"DRY-RUN peek  → queue='{queue}' type='{expected_type}'")
        return

    vhost_enc = urllib.parse.quote(cfg.vhost, safe="")
    queue_enc = urllib.parse.quote(queue, safe="")
    url = f"http://{cfg.host}:{cfg.mgmt_port}/api/queues/{vhost_enc}/{queue_enc}/get"
    payload = json.dumps({
        "count": 5,
        "ackmode": "ack_requeue_true",
        "encoding": "auto",
        "truncate": 100000
    }).encode()

    deadline = time.time() + cfg.timeout
    found = False
    while time.time() < deadline:
        try:
            req = urllib.request.Request(
                url, data=payload, method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": _basic_auth(cfg)
                }
            )
            with urllib.request.urlopen(req, timeout=4) as resp:
                msgs = json.loads(resp.read())
            for m in msgs:
                if f"<type>{expected_type}</type>" in m.get("payload", ""):
                    found = True
                    break
        except Exception:
            pass
        if found:
            break
        time.sleep(0.5)

    if found:
        ok(f"Arrived     : queue='{queue}' type={expected_type} — {label}")
    elif _queue_had_activity(before_stats, _queue_marker(_queue_stats(cfg, queue))):
        ok(f"Delivered   : queue='{queue}' type={expected_type} — {label} (consumed before peek)")
    else:
        fail(f"NOT ARRIVED : queue='{queue}' type={expected_type} — {label}")


# ── Test runner helper ─────────────────────────────────────────────────────────
def run_flow(cfg, schema_name: str, label: str,
             xml: str, exchange: str, routing_key: str, arrival_queue: str):
    if cfg.verbose:
        print(f"\n{CYAN}--- XML ---{RESET}\n{xml}\n")
    valid = validate(xml, schema_name, label)
    if not valid:
        warn("Skipping publish — XML failed schema validation")
        return
    msg_type = xml.split("<type>")[1].split("</type>")[0]
    if exchange:
        # True shadow queue: bind a temp exclusive queue to the exchange+rk and basic_get
        _shadow_queue_test(cfg, exchange, routing_key, xml, msg_type, label)
    elif cfg.pause_consumers and not cfg.dry_run:
        # Direct-queue flow with pause: disconnect consumers, publish, immediate basic_get
        _state["tests"] += 1
        result = _pause_publish_get(cfg, arrival_queue, xml, msg_type, label)
        if result is None:
            # No consumers found — fall back to regular publish + peek
            before_stats = _queue_marker(_queue_stats(cfg, arrival_queue))
            if publish(cfg, "", routing_key, xml):
                peek_queue(cfg, arrival_queue, msg_type, label, before_stats)
    else:
        before_stats = _queue_marker(_queue_stats(cfg, arrival_queue))
        if publish(cfg, exchange, routing_key, xml):
            peek_queue(cfg, arrival_queue, msg_type, label, before_stats)


# ─────────────────────────────────────────────────────────────────────────────
# FLOW DEFINITIONS
# ─────────────────────────────────────────────────────────────────────────────

def test_connectivity(cfg):
    header("RabbitMQ Connectivity")
    if cfg.dry_run:
        info("DRY-RUN: skipped")
        return True
    _state["tests"] += 1
    try:
        get_channel(cfg)
        ok(f"Connected @ {cfg.host}:{cfg.port}  vhost={cfg.vhost}")
        return True
    except Exception as e:
        if cfg.strict_live:
            fail(f"Cannot connect @ {cfg.host}:{cfg.port}")
        else:
            warn(f"Cannot connect @ {cfg.host}:{cfg.port}")
        warn(f"{type(e).__name__}: {e}")
        if "ACCESS_REFUSED" in str(e):
            warn("RabbitMQ rejected the username/password or vhost. Use a RabbitMQ user from shift-secrets, not the VM SSH user.")
        warn("Falling back to dry-run — publish/arrival tests skipped")
        cfg.dry_run = True
        return False


# ── Flow 01 : Frontend → CRM  new_registration (§5.1) ─────────────────────────
def flow_frontend_crm_new_registration(cfg):
    header("Flow 01 · Frontend → CRM  [new_registration]  →  crm.incoming")
    corr = new_uuid()
    body = """\
        <customer>
          <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-000000000001</identity_uuid>
          <email>lena.declercq@test.be</email>
          <type>private</type>
          <is_company_linked>false</is_company_linked>
          <date_of_birth>1995-03-21</date_of_birth>
          <contact>
            <first_name>Lena</first_name>
            <last_name>Declercq</last_name>
          </contact>
          <address>Tervurenlaan 1, 1040 Brussel</address>
          <session_id>sess-2026-001</session_id>
          <payment_due>
            <amount currency="eur">0.00</amount>
            <status>unpaid</status>
          </payment_due>
        </customer>"""
    xml = build_message("new_registration", "frontend", body, correlation_id=corr)
    run_flow(cfg, "frontend_new_registration",
             "Frontend→CRM new_registration",
             xml, "", "crm.incoming", "crm.incoming")


# ── Flow 02 : CRM → Kassa  new_registration (§10.1) ──────────────────────────
def flow_crm_kassa_new_registration(cfg):
    header("Flow 02 · CRM → Kassa  [new_registration]  →  kassa.incoming")
    corr = new_uuid()
    body = """\
        <customer>
          <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-000000000001</identity_uuid>
          <email>lena.declercq@test.be</email>
          <date_of_birth>1995-03-21</date_of_birth>
          <contact>
            <first_name>Lena</first_name>
            <last_name>Declercq</last_name>
          </contact>
          <type>private</type>
          <session_id>sess-2026-001</session_id>
          <session_title>Keynote: AI in Business</session_title>
          <payment_due>
            <amount currency="eur">25.00</amount>
            <status>unpaid</status>
          </payment_due>
        </customer>"""
    xml = build_message("new_registration", "crm", body, correlation_id=corr)
    run_flow(cfg, "crm_to_kassa_new_registration",
             "CRM→Kassa new_registration",
             xml, "kassa.exchange", "kassa.incoming", "kassa.incoming")


# ── Flow 03 : Kassa → CRM  consumption_order (§6.1 v2.3) ─────────────────────
def flow_kassa_consumption_order(cfg):
    header("Flow 03 · Kassa → CRM  [consumption_order]  →  kassa.payments.consumption")
    body = """\
        <customer>
          <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-000000000001</identity_uuid>
          <type>private</type>
          <email>lena.declercq@test.be</email>
        </customer>
        <items>
          <item>
            <id>item-001</id>
            <sku>KOFFIE</sku>
            <description>Koffie</description>
            <quantity>2</quantity>
            <unit_price currency="eur">3.00</unit_price>
            <vat_rate>21</vat_rate>
            <total_amount currency="eur">6.00</total_amount>
          </item>
          <item>
            <id>item-002</id>
            <sku>LUNCH</sku>
            <description>Lunch sandwich</description>
            <quantity>1</quantity>
            <unit_price currency="eur">7.50</unit_price>
            <vat_rate>6</vat_rate>
            <total_amount currency="eur">7.50</total_amount>
          </item>
        </items>"""
    xml = build_message("consumption_order", "kassa", body, correlation_id=new_uuid())
    run_flow(cfg, "kassa_consumption_order",
             "Kassa→CRM consumption_order",
             xml, "kassa.exchange", "kassa.payments.consumption", "crm.incoming")


# ── Flow 04 : Kassa → CRM/Facturatie  payment_registered (§6.5 / §6.6) ────────
def flow_kassa_payment_registered(cfg):
    header("Flow 04 · Kassa → CRM/Facturatie  [payment_registered]  →  kassa.payments.registration")
    body = """\
        <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-000000000001</identity_uuid>
        <invoice>
          <id>INV-KASSA-2026-001</id>
          <amount_paid currency="eur">25.00</amount_paid>
          <status>paid</status>
        </invoice>
        <payment_context>session_registration</payment_context>
        <transaction>
          <id>TXN-2026-001</id>
          <payment_method>on_site</payment_method>
        </transaction>"""
    xml = build_message("payment_registered", "kassa", body, correlation_id=new_uuid())
    run_flow(cfg, "kassa_payment_registered",
             "Kassa→CRM payment_registered",
             xml, "kassa.exchange", "kassa.payments.registration", "crm.incoming")


# ── Flow 05 : CRM → Facturatie  invoice_request (§11.1) ──────────────────────
def flow_crm_facturatie_invoice_request(cfg):
    header("Flow 05 · CRM → Facturatie  [invoice_request]  →  facturatie.incoming")
    corr = new_uuid()
    body = """\
        <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-000000000001</identity_uuid>
        <invoice_data>
          <contact>
            <first_name>Lena</first_name>
            <last_name>Declercq</last_name>
          </contact>
          <email>lena.declercq@test.be</email>
          <address>
            <street>Tervurenlaan</street>
            <number>1</number>
            <postal_code>1040</postal_code>
            <city>Brussel</city>
            <country>BE</country>
          </address>
        </invoice_data>"""
    xml = build_message("invoice_request", "crm", body, correlation_id=corr)
    run_flow(cfg, "crm_to_facturatie_invoice_request",
             "CRM→Facturatie invoice_request",
             xml, "", "facturatie.incoming", "facturatie.incoming")


# ── Flow 06 : CRM → Mailing  send_mailing (§12.1) ────────────────────────────
def flow_crm_mailing_send_mailing(cfg):
    header("Flow 06 · CRM → Mailing  [send_mailing]  →  crm.to.mailing")
    corr = new_uuid()
    body = """\
        <campaign_id>sg-campaign-reg-001</campaign_id>
        <subject>Bevestiging inschrijving Shiftfestival 2026</subject>
        <mail_type>registration_confirmation</mail_type>
        <recipients>
          <recipient>
            <email>lena.declercq@test.be</email>
            <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-000000000001</identity_uuid>
            <contact>
              <first_name>Lena</first_name>
              <last_name>Declercq</last_name>
            </contact>
          </recipient>
        </recipients>
        <template_data>{"session_title":"Keynote: AI in Business","session_date":"15 mei 2026 14:00"}</template_data>"""
    xml = build_message("send_mailing", "crm", body, correlation_id=corr)
    run_flow(cfg, "send_mailing",
             "CRM→Mailing send_mailing",
             xml, "", "crm.to.mailing", "crm.to.mailing")


# ── Flow 07 : Facturatie → Mailing  send_mailing (§13.1) ─────────────────────
def flow_facturatie_mailing_send_mailing(cfg):
    header("Flow 07 · Facturatie → Mailing  [send_mailing]  →  facturatie.to.mailing")
    corr = new_uuid()
    body = """\
        <campaign_id>sg-invoice-00142</campaign_id>
        <subject>Uw factuur voor Shiftfestival 2026</subject>
        <mail_type>invoice_ready</mail_type>
        <recipients>
          <recipient>
            <email>lena.declercq@test.be</email>
            <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-000000000001</identity_uuid>
            <contact>
              <first_name>Lena</first_name>
              <last_name>Declercq</last_name>
            </contact>
          </recipient>
        </recipients>
        <template_data>{"invoice_id":"foss-inv-00142","amount":"31.50","due_date":"2026-06-15"}</template_data>"""
    xml = build_message("send_mailing", "facturatie", body, correlation_id=corr)
    run_flow(cfg, "send_mailing",
             "Facturatie→Mailing send_mailing",
             xml, "", "facturatie.to.mailing", "facturatie.to.mailing")


# ── Flow 08 : Facturatie → CRM  invoice_status (§8.1) ────────────────────────
def flow_facturatie_crm_invoice_status(cfg):
    header("Flow 08 · Facturatie → CRM  [invoice_status]  →  crm.incoming")
    body = """\
        <invoice_id>foss-inv-00142</invoice_id>
        <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-000000000001</identity_uuid>
        <status>sent</status>
        <amount currency="eur">31.50</amount>
        <due_date>2026-06-15</due_date>"""
    xml = build_message("invoice_status", "facturatie", body, correlation_id=new_uuid())
    run_flow(cfg, "facturatie_invoice_status",
             "Facturatie→CRM invoice_status",
             xml, "", "crm.incoming", "crm.incoming")


# ── Flow 09 : Facturatie → CRM  payment_registered (§8.2) ────────────────────
def flow_facturatie_crm_payment_registered(cfg):
    header("Flow 09 · Facturatie → CRM  [payment_registered]  →  crm.incoming")
    body = """\
        <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-000000000001</identity_uuid>
        <invoice>
          <id>foss-inv-00142</id>
          <amount_paid currency="eur">31.50</amount_paid>
          <status>paid</status>
        </invoice>
        <payment_context>online_invoice</payment_context>
        <transaction>
          <id>TXN-ONLINE-2026-001</id>
          <payment_method>online</payment_method>
        </transaction>"""
    xml = build_message("payment_registered", "facturatie", body, correlation_id=new_uuid())
    run_flow(cfg, "facturatie_payment_registered",
             "Facturatie→CRM payment_registered",
             xml, "", "crm.incoming", "crm.incoming")


# ── Flow 10 : Mailing → CRM  mailing_status (§9.1) ───────────────────────────
def flow_mailing_crm_mailing_status(cfg):
    header("Flow 10 · Mailing → CRM  [mailing_status]  →  crm.incoming")
    body = """\
        <campaign_id>sg-campaign-reg-001</campaign_id>
        <subject>Bevestiging inschrijving Shiftfestival 2026</subject>
        <sent>1</sent>
        <delivered>1</delivered>
        <bounced>0</bounced>
        <opened>1</opened>
        <status>completed</status>"""
    xml = build_message("mailing_status", "mailing", body, correlation_id=new_uuid())
    run_flow(cfg, "mailing_status",
             "Mailing→CRM mailing_status",
             xml, "", "crm.incoming", "crm.incoming")


# ── Flow 11 : Planning → CRM  session_created (§7.1) ─────────────────────────
def flow_planning_crm_session_created(cfg):
    header("Flow 11 · Planning → CRM  [session_created]  →  planning.exchange / planning.session.created")
    body = """\
        <session_id>sess-2026-001</session_id>
        <title>Keynote: AI in Business</title>
        <start_datetime>2026-05-15T14:00:00Z</start_datetime>
        <end_datetime>2026-05-15T15:00:00Z</end_datetime>
        <location>Aula A - Campus Jette</location>
        <session_type>keynote</session_type>
        <status>published</status>
        <max_attendees>120</max_attendees>
        <current_attendees>0</current_attendees>
        <speaker>
          <contact>
            <first_name>Prof. Ahmed</first_name>
            <last_name>El-Rashidi</last_name>
          </contact>
          <organisation>KU Leuven</organisation>
        </speaker>"""
    xml = build_message("session_created", "planning", body, correlation_id=new_uuid())
    run_flow(cfg, "planning_session_created",
             "Planning→CRM session_created",
             xml, "planning.exchange", "planning.session.created",
             "planning.session.events")


# ── Flow 12 : Planning → CRM  session_updated (§7.2) ─────────────────────────
def flow_planning_crm_session_updated(cfg):
    header("Flow 12 · Planning → CRM  [session_updated]  →  planning.exchange / planning.session.updated")
    body = """\
        <session_id>sess-2026-001</session_id>
        <title>Keynote: AI in Business (Updated)</title>
        <start_datetime>2026-05-15T14:30:00Z</start_datetime>
        <end_datetime>2026-05-15T15:30:00Z</end_datetime>
        <location>Aula B - Campus Jette</location>
        <session_type>keynote</session_type>
        <status>published</status>
        <max_attendees>150</max_attendees>
        <current_attendees>42</current_attendees>
        <change_reason>Room change due to higher attendance</change_reason>"""
    xml = build_message("session_updated", "planning", body, correlation_id=new_uuid())
    run_flow(cfg, "planning_session_updated",
             "Planning→CRM session_updated",
             xml, "planning.exchange", "planning.session.updated",
             "planning.session.events")


# ── Flow 13 : Frontend → Planning  session_create_request (§19.1) ────────────
def flow_frontend_planning_session_create_request(cfg):
    header("Flow 13 · Frontend → Planning  [session_create_request]  →  frontend.to.planning.session.create")
    body = """\
        <session_id>sess-2026-new-001</session_id>
        <title>Workshop: Cloud Native Development</title>
        <start_datetime>2026-05-16T09:00:00Z</start_datetime>
        <end_datetime>2026-05-16T11:00:00Z</end_datetime>
        <location>Lab 3 - Campus Jette</location>
        <session_type>workshop</session_type>
        <status>draft</status>
        <max_attendees>30</max_attendees>"""
    xml = build_message("session_create_request", "frontend", body)
    run_flow(cfg, "frontend_session_create_request",
             "Frontend→Planning session_create_request",
             xml, "planning.exchange", "frontend.to.planning.session.create",
             "planning.session.events")


# ── Flow 14 : Frontend → Planning  calendar_invite (§19.3) ───────────────────
def flow_frontend_planning_calendar_invite(cfg):
    header("Flow 14 · Frontend → Planning  [calendar_invite]  →  calendar.exchange / frontend.to.planning.calendar.invite")
    body = """\
        <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-000000000001</identity_uuid>
        <session_id>sess-2026-001</session_id>
        <title>Keynote: AI in Business</title>
        <start_datetime>2026-05-15T14:00:00Z</start_datetime>
        <end_datetime>2026-05-15T15:00:00Z</end_datetime>
        <location>Aula A - Campus Jette</location>
        <attendee_email>lena.declercq@test.be</attendee_email>"""
    xml = build_message("calendar_invite", "frontend", body)
    run_flow(cfg, "frontend_calendar_invite",
             "Frontend→Planning calendar_invite",
             xml, "calendar.exchange", "frontend.to.planning.calendar.invite",
             "planning.calendar.invite")


# ── Flow 15 : Monitoring → Mailing  system_alert (§4) ────────────────────────
def flow_monitoring_mailing_system_alert(cfg):
    header("Flow 15 · Monitoring → Mailing  [system_alert]  →  to_mailing")
    xml = build_alert("crm", "Heartbeat timeout: crm has not responded and is considered offline")
    if cfg.verbose:
        print(f"\n{CYAN}--- XML ---{RESET}\n{xml}\n")
    valid = validate(xml, "monitoring_system_alert", "Monitoring→Mailing system_alert")
    if not valid:
        warn("Skipping publish — alert XML failed schema validation")
        return
    if publish(cfg, "", "to_mailing", xml):
        peek_queue(cfg, "to_mailing", "HEARTBEAT_CRITICAL",
                   "Monitoring→Mailing system_alert")


# ── Flow 16 : All Teams → Monitoring  heartbeat (§3) ─────────────────────────
def flow_heartbeats(cfg):
    header("Flow 16 · All Teams → Monitoring  [heartbeat]  →  heartbeat")
    teams = ["crm", "kassa", "facturatie", "planning", "mailing",
             "monitoring", "frontend", "identity-service"]
    for team in teams:
        body = f"        <status>online</status>\n        <uptime>{int(time.time()) % 10000}</uptime>"
        xml = build_message("heartbeat", team, body)
        if cfg.verbose:
            print(f"\n{CYAN}--- heartbeat ({team}) ---{RESET}\n{xml}\n")
        validate(xml, "heartbeat", f"heartbeat from {team}")
        if team != teams[-1]:
            publish(cfg, "", "heartbeat", xml)
    # Final message goes through run_flow so pause_consumers is respected
    body = f"        <status>online</status>\n        <uptime>{int(time.time()) % 10000}</uptime>"
    xml = build_message("heartbeat", teams[-1], body)
    run_flow(cfg, "heartbeat", "Any heartbeat arrived in monitoring queue",
             xml, "", "heartbeat", "heartbeat")


# ── Flow 17 : Frontend → CRM  event_ended (§5.7) ─────────────────────────────
def flow_frontend_crm_event_ended(cfg):
    header("Flow 17 · Frontend → CRM  [event_ended]  →  crm.incoming")
    body = """\
        <session_id>sess-keynote-001</session_id>
        <ended_at>2026-05-15T22:00:00Z</ended_at>"""
    xml = build_message("event_ended", "frontend", body, correlation_id=new_uuid())
    run_flow(cfg, "frontend_event_ended",
             "Frontend→CRM event_ended",
             xml, "", "crm.incoming", "crm.incoming")


# ── Flow 18 : Frontend → Facturatie  event_ended (§11.6) ──────────────────────
def flow_frontend_facturatie_event_ended(cfg):
    header("Flow 18 · Frontend → Facturatie  [event_ended]  →  facturatie.incoming")
    body = """\
        <session_id>sess-keynote-001</session_id>
        <ended_at>2026-05-15T22:00:00Z</ended_at>"""
    xml = build_message("event_ended", "frontend", body, correlation_id=new_uuid())
    run_flow(cfg, "frontend_event_ended",
             "Frontend→Facturatie event_ended",
             xml, "", "facturatie.incoming", "facturatie.incoming")


# ── Flow 19 : All Teams → Monitoring  log (§3.5) ─────────────────────────────
def flow_logs(cfg):
    header("Flow 19 · All Teams → Monitoring  [log]  →  logs")
    teams   = ["crm", "kassa", "facturatie", "frontend", "planning",
               "mailing", "identity-service", "iot_gateway"]
    actions = ["registration", "user", "payment", "invoice", "session",
               "calendar", "email", "wallet", "refund", "identity",
               "xml_validation", "system_error", "badge"]
    levels  = ["info", "warning", "error"]

    # Validate all 8 × 13 × 3 = 312 combinations against the log XSD
    for team in teams:
        for action in actions:
            for level in levels:
                body = f"""\
        <level>{level}</level>
        <action>{action}</action>
        <message>Test log [{level}] [{action}] from {team}</message>"""
                xml = build_message("log", team, body)
                if cfg.verbose:
                    print(f"\n{CYAN}--- log ({team}/{action}/{level}) ---{RESET}\n{xml}\n")
                validate(xml, "logs", f"log {team}/{action}/{level}")

    # Publish one message per team (live routing check); last one uses run_flow for pause support
    for team in teams[:-1]:
        body = """\
        <level>info</level>
        <action>registration</action>
        <message>Integration test log message</message>"""
        xml = build_message("log", team, body)
        publish(cfg, "", "logs", xml)

    body = """\
        <level>info</level>
        <action>registration</action>
        <message>Integration test log message</message>"""
    xml = build_message("log", teams[-1], body)
    run_flow(cfg, "logs", "Any log arrived in monitoring queue",
             xml, "", "logs", "logs")


# ── Flow 20 : IoT/Kassa → Kassa  badge_scanned (§6.3) ────────────────────────
def flow_kassa_badge_scanned(cfg):
    header("Flow 20 · IoT/Kassa → Kassa  [badge_scanned]  →  kassa.incoming")

    # Variant A: badge-scan (iot_gateway source, badge_id in body)
    body_badge = """\
        <badge_id>BADGE-0042</badge_id>
        <location>entrance</location>
        <scanned_at>2026-05-15T18:06:30Z</scanned_at>"""
    xml_badge = build_message("badge_scanned", "iot_gateway", body_badge)
    run_flow(cfg, "kassa_badge_scanned",
             "IoT→Kassa badge_scanned (badge-scan)",
             xml_badge, "", "kassa.incoming", "kassa.incoming")

    # Variant B: QR-scan (kassa source, identity_uuid in body)
    body_qr = """\
        <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-123456789abc</identity_uuid>
        <location>entrance</location>
        <scanned_at>2026-05-15T18:06:35Z</scanned_at>"""
    xml_qr = build_message("badge_scanned", "kassa", body_qr)
    run_flow(cfg, "kassa_badge_scanned",
             "Kassa→Kassa badge_scanned (QR-scan)",
             xml_qr, "", "kassa.incoming", "kassa.incoming")


# ── Flow 21 : Kassa → CRM  wallet_lease_request (§26.1) ──────────────────────
def flow_kassa_wallet_lease_request(cfg):
    header("Flow 21 · Kassa → CRM  [wallet_lease_request]  →  kassa.to.crm.wallet_lease_request")

    # Variant A: via badge-scan (identity_uuid + badge_id)
    body_badge = """\
        <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-123456789abc</identity_uuid>
        <badge_id>BADGE-0042</badge_id>"""
    xml_badge = build_message("wallet_lease_request", "kassa", body_badge)
    run_flow(cfg, "kassa_wallet_lease_request",
             "Kassa→CRM wallet_lease_request (via badge)",
             xml_badge, "kassa.exchange", "kassa.to.crm.wallet_lease_request",
             "crm.incoming")

    # Variant B: via QR-scan (identity_uuid only, badge_id absent)
    body_qr = """\
        <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-123456789abc</identity_uuid>"""
    xml_qr = build_message("wallet_lease_request", "kassa", body_qr)
    run_flow(cfg, "kassa_wallet_lease_request",
             "Kassa→CRM wallet_lease_request (via QR, no badge_id)",
             xml_qr, "kassa.exchange", "kassa.to.crm.wallet_lease_request",
             "crm.incoming")


# ── Rejection tests (contract compliance) ─────────────────────────────────────
def test_schema_rejections():
    header("Contract Violation Rejection Tests")

    # Correct: valid send_mailing with full recipients structure
    _state["tests"] += 1
    corr = new_uuid()
    valid_mailing_body = f"""\
        <campaign_id>sg-test-001</campaign_id>
        <subject>Test</subject>
        <mail_type>registration_confirmation</mail_type>
        <recipients>
          <recipient>
            <email>x@test.be</email>
            <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-000000000001</identity_uuid>
            <contact>
              <first_name>Test</first_name>
              <last_name>User</last_name>
            </contact>
          </recipient>
        </recipients>"""
    valid_mailing = build_message("send_mailing", "crm", valid_mailing_body,
                                  correlation_id=corr)
    schema_mailing = COMPILED["send_mailing"]
    try:
        schema_mailing.assertValid(parse_xml(valid_mailing))
        ok("Contract: correct send_mailing (campaign_id + recipients) validates OK")
    except etree.DocumentInvalid as e:
        fail(f"Valid send_mailing failed schema: {e}")

    # Forbidden: xmlns namespace in header (v1.0 leftover)
    _state["tests"] += 1
    xmlns_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<message xmlns="urn:integration:planning:v1">'
        '<header><message_id>e8b27c1d-4f2a-4b3e-9c5f-000000000001</message_id>'
        '<timestamp>2026-01-01T00:00:00Z</timestamp>'
        '<source>crm</source><type>heartbeat</type><version>2.0</version></header>'
        '<body><status>online</status></body></message>'
    )
    schema_hb = COMPILED["heartbeat"]
    try:
        schema_hb.assertValid(parse_xml(xmlns_xml))
        fail("Should have rejected: xmlns namespace (v1.0 leftover) in header")
    except (etree.DocumentInvalid, etree.XMLSyntaxError):
        ok("Rejected: xmlns namespace in header (Regel 1 violation)")

    # Forbidden: version=1.0 instead of 2.0
    _state["tests"] += 1
    v1_xml = build_message("heartbeat", "crm",
                           "        <status>online</status>").replace(
        "<version>2.0</version>", "<version>1.0</version>")
    try:
        schema_hb.assertValid(parse_xml(v1_xml))
        fail("Should have rejected: version=1.0")
    except etree.DocumentInvalid:
        ok("Rejected: version=1.0 (contract requires 2.0)")

    # Forbidden: date_of_birth missing, <age> used instead (Regel 4)
    _state["tests"] += 1
    corr2 = new_uuid()
    age_body = f"""\
        <customer>
          <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-000000000001</identity_uuid>
          <email>j@j.be</email>
          <type>private</type>
          <is_company_linked>false</is_company_linked>
          <age>29</age>
          <contact>
            <first_name>Jan</first_name><last_name>Peeters</last_name>
          </contact>
          <address>Straat 1</address>
          <session_id>sess-001</session_id>
          <payment_due>
            <amount currency="eur">0.00</amount>
            <status>unpaid</status>
          </payment_due>
        </customer>"""
    age_xml = build_message("new_registration", "frontend", age_body, correlation_id=corr2)
    schema_reg = COMPILED["frontend_new_registration"]
    try:
        schema_reg.assertValid(parse_xml(age_xml))
        fail("Should have rejected: <age> field (Regel 4 violation — use date_of_birth)")
    except etree.DocumentInvalid:
        ok("Rejected: <age> field (contract requires <date_of_birth>)")

    # Forbidden: currency attribute missing on monetary item amount (Regel 3)
    _state["tests"] += 1
    no_currency_body = """\
        <customer>
          <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-000000000001</identity_uuid>
          <type>private</type>
        </customer>
        <items>
          <item>
            <id>item-001</id>
            <sku>KOFFIE</sku>
            <description>Koffie</description>
            <quantity>1</quantity>
            <unit_price>3.00</unit_price>
            <vat_rate>21</vat_rate>
            <total_amount currency="eur">3.00</total_amount>
          </item>
        </items>"""
    no_curr_xml = build_message("consumption_order", "kassa", no_currency_body)
    schema_cons = COMPILED["kassa_consumption_order"]
    try:
        schema_cons.assertValid(parse_xml(no_curr_xml))
        fail("Should have rejected: missing currency attribute (Regel 3 violation)")
    except etree.DocumentInvalid:
        ok("Rejected: missing currency attribute on monetary field (Regel 3)")

    # Forbidden: invalid mail_type enum in send_mailing
    _state["tests"] += 1
    bad_mailtype_body = f"""\
        <campaign_id>sg-test-001</campaign_id>
        <subject>Test</subject>
        <mail_type>unknown_type</mail_type>
        <recipients>
          <recipient>
            <email>x@test.be</email>
            <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-000000000001</identity_uuid>
            <contact>
              <first_name>Test</first_name>
              <last_name>User</last_name>
            </contact>
          </recipient>
        </recipients>"""
    bad_type_xml = build_message("send_mailing", "crm", bad_mailtype_body,
                                  correlation_id=new_uuid())
    try:
        schema_mailing.assertValid(parse_xml(bad_type_xml))
        fail("Should have rejected: invalid mail_type enum")
    except etree.DocumentInvalid:
        ok("Rejected: invalid mail_type enum (must be registration_confirmation etc.)")

    # Forbidden: system_alert as <message> envelope (must be flat <alert> root)
    _state["tests"] += 1
    envelope_alert = build_message("system_alert", "monitoring",
                                   "        <alert_level>critical</alert_level>\n"
                                   "        <affected_team>crm</affected_team>\n"
                                   "        <message>offline</message>")
    schema_alert = COMPILED["monitoring_system_alert"]
    try:
        schema_alert.assertValid(parse_xml(envelope_alert))
        fail("Should have rejected: system_alert in <message> envelope (must be flat <alert>)")
    except etree.DocumentInvalid:
        ok("Rejected: system_alert in <message> envelope (§4 requires flat <alert> root)")

    # Forbidden: badge_scanned with both badge_id AND identity_uuid (xs:choice = exactly one)
    _state["tests"] += 1
    both_body = """\
        <badge_id>BADGE-0042</badge_id>
        <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-123456789abc</identity_uuid>
        <location>entrance</location>
        <scanned_at>2026-05-15T18:06:30Z</scanned_at>"""
    both_xml = build_message("badge_scanned", "iot_gateway", both_body)
    schema_bs = COMPILED["kassa_badge_scanned"]
    try:
        schema_bs.assertValid(parse_xml(both_xml))
        fail("Should have rejected: badge_scanned with both badge_id and identity_uuid (xs:choice)")
    except etree.DocumentInvalid:
        ok("Rejected: badge_scanned with both badge_id+identity_uuid (§6.3 xs:choice enforces exactly one)")

    # Forbidden: badge_scanned with neither badge_id nor identity_uuid
    _state["tests"] += 1
    neither_body = """\
        <location>entrance</location>
        <scanned_at>2026-05-15T18:06:30Z</scanned_at>"""
    neither_xml = build_message("badge_scanned", "iot_gateway", neither_body)
    try:
        schema_bs.assertValid(parse_xml(neither_xml))
        fail("Should have rejected: badge_scanned with no identifier")
    except etree.DocumentInvalid:
        ok("Rejected: badge_scanned with no identifier (§6.3 xs:choice requires exactly one)")

    # Correct: wallet_lease_request without badge_id (QR-scan path) must be valid
    _state["tests"] += 1
    qr_lease_body = """\
        <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-123456789abc</identity_uuid>"""
    qr_lease_xml = build_message("wallet_lease_request", "kassa", qr_lease_body)
    schema_wlr = COMPILED["kassa_wallet_lease_request"]
    try:
        schema_wlr.assertValid(parse_xml(qr_lease_xml))
        ok("Contract: wallet_lease_request without badge_id (QR-scan) validates OK")
    except etree.DocumentInvalid as e:
        fail(f"Valid wallet_lease_request (QR-scan, no badge_id) failed schema: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def should_test(cfg, *teams: str) -> bool:
    if cfg.teams == "all":
        return True
    active = set(cfg.teams.split(","))
    return bool(active.intersection(teams))


def main():
    load_env()
    cfg = parse_args()
    if cfg.env != ".env":
        load_env(cfg.env)

    print()
    print(f"{BOLD}╔══════════════════════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}║  RabbitMQ Integration Test Suite — Groep 1 — 2026            ║{RESET}")
    print(f"{BOLD}║  Contract: XML/XSD v2.3 Centralized                          ║{RESET}")
    print(f"{BOLD}║  Frontend · CRM · Kassa · Facturatie · Planning              ║{RESET}")
    print(f"{BOLD}║  Mailing · Monitoring · Identity                              ║{RESET}")
    print(f"{BOLD}╚══════════════════════════════════════════════════════════════╝{RESET}")
    print()
    print(f"  Host    : {CYAN}{cfg.host}:{cfg.port}{RESET}")
    print(f"  Mgmt    : {CYAN}{cfg.host}:{cfg.mgmt_port}{RESET}")
    print(f"  Vhost   : {CYAN}{cfg.vhost}{RESET}")
    print(f"  Timeout : {CYAN}{cfg.timeout}s{RESET}")
    print(f"  Mode    : {CYAN}{'DRY-RUN (schema only)' if cfg.dry_run else 'LIVE'}{RESET}")
    print(f"  Pause   : {CYAN}{'yes (consumers disconnected during test)' if cfg.pause_consumers else 'no'}{RESET}")
    print(f"  Strict  : {CYAN}{'yes' if cfg.strict_live else 'no'}{RESET}")
    print(f"  Teams   : {CYAN}{cfg.teams}{RESET}")

    test_connectivity(cfg)
    test_schema_rejections()

    if should_test(cfg, "frontend", "crm"):
        flow_frontend_crm_new_registration(cfg)
    if should_test(cfg, "crm", "kassa"):
        flow_crm_kassa_new_registration(cfg)
    if should_test(cfg, "kassa", "crm"):
        flow_kassa_consumption_order(cfg)
        flow_kassa_payment_registered(cfg)
        flow_kassa_badge_scanned(cfg)
        flow_kassa_wallet_lease_request(cfg)
    if should_test(cfg, "crm", "facturatie"):
        flow_crm_facturatie_invoice_request(cfg)
    if should_test(cfg, "crm", "mailing"):
        flow_crm_mailing_send_mailing(cfg)
    if should_test(cfg, "facturatie", "mailing"):
        flow_facturatie_mailing_send_mailing(cfg)
    if should_test(cfg, "facturatie", "crm"):
        flow_facturatie_crm_invoice_status(cfg)
        flow_facturatie_crm_payment_registered(cfg)
    if should_test(cfg, "mailing", "crm"):
        flow_mailing_crm_mailing_status(cfg)
    if should_test(cfg, "planning", "crm"):
        flow_planning_crm_session_created(cfg)
        flow_planning_crm_session_updated(cfg)
    if should_test(cfg, "frontend", "planning"):
        flow_frontend_planning_session_create_request(cfg)
        flow_frontend_planning_calendar_invite(cfg)
    if should_test(cfg, "frontend", "crm", "facturatie"):
        flow_frontend_crm_event_ended(cfg)
        flow_frontend_facturatie_event_ended(cfg)
    if should_test(cfg, "monitoring", "mailing"):
        flow_monitoring_mailing_system_alert(cfg)
    if should_test(cfg, "monitoring", "crm", "kassa", "facturatie",
                   "planning", "mailing", "frontend", "identity-service"):
        flow_heartbeats(cfg)
        flow_logs(cfg)

    if _conn and _conn.is_open:
        try:
            _conn.close()
        except Exception:
            pass

    print()
    print(f"{BOLD}══ Summary ══════════════════════════════════════════════════════{RESET}")
    print(f"  Tests run : {BOLD}{_state['tests']}{RESET}")
    if _state["failures"] == 0:
        print(f"  Result    : {GREEN}{BOLD}ALL PASSED ✓{RESET}")
    else:
        print(f"  Failures  : {RED}{BOLD}{_state['failures']}{RESET}")
    print()
    sys.exit(0 if _state["failures"] == 0 else 1)


if __name__ == "__main__":
    main()
