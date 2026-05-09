# XML Test Environment — Groep 1

Centrale testhub voor alle XML-berichtflows binnen het integratieproject.

---

## Scripts

| Script | Doel | Wanneer |
|---|---|---|
| `test_contract_full.py` | Importeert echte team builder-functies, bouwt XML, valideert tegen team-XSDs + contract-regels | Builder compliance |
| `test_integration.py` | Verstuurt XML naar de live RabbitMQ op de VM, verifieert routing via shadow queues | VM routing |
| `check_rabbit.py` | Snelle connectivity check | Debuggen |
| `setup_queues.py` | Configureert exchanges en queues op een schone RabbitMQ | Eenmalig / reset |

---

## `test_contract_full.py`

```bash
# Lokaal — alle teams (builders testen, geen VM nodig):
python test_contract_full.py --phase1-only

# Alleen Kassa:
python test_contract_full.py --phase1-only --teams kassa

# Volledig (builder + routing tegen VM):
python test_contract_full.py \
  --host 20.126.113.148 --port 30000 --user guest --pass guest
```

| Optie | Standaard | Beschrijving |
|---|---|---|
| `--repos-dir DIR` | bovenliggende map van dit script | Root waar team repos als subdirs staan (`Kassa/`, `Planning/`, `Facturatie/`) |
| `--phase1-only` | — | Alleen builder compliance, geen VM verbinding |
| `--phase2-only` | — | Alleen routing verificatie |
| `--teams LIST` | `all` | Komma-gescheiden: `kassa,planning,facturatie` |
| `--verbose` | — | Print volledige XML output |

## `test_integration.py`

```bash
# Azure VM (standaard instellingen):
python test_integration.py

# Andere host:
python test_integration.py --host localhost --port 5672 --mgmt-port 15672

# Alleen validatie, niet versturen:
python test_integration.py --dry-run

# Specifiek team:
python test_integration.py --teams kassa,planning
```

---

## GitHub Actions

### Vereiste secrets

Instellen via **GitHub repo → Settings → Secrets and variables → Actions → New repository secret**.

| Secret | Waarde | Waar te vinden |
|---|---|---|
| `RABBIT_HOST` | Azure VM publiek IP | Azure Portal → VM → Public IP |
| `RABBIT_PORT` | AMQP NodePort | `30000` (standaard voor dit project) |
| `RABBIT_MGMT_PORT` | Management API NodePort | `30001` (standaard voor dit project) |
| `RABBIT_USER` | RabbitMQ gebruikersnaam | `setup/.env` op de VM |
| `RABBIT_PASS` | RabbitMQ wachtwoord | `setup/.env` op de VM |

> Geen `ORG_READ_TOKEN` nodig — alle team repos zijn public.

### Twee CI jobs

**Job 1 — Builder Compliance** (elke push/PR):
- Checkt Kassa, Planning en Facturatie repos uit (public, geen token)
- Runt `test_contract_full.py --phase1-only --repos-dir $GITHUB_WORKSPACE`
- Geeft een job summary met pass/fail per builder

**Job 2 — VM Routing** (alleen main branch of manueel):
- Runt `test_integration.py` tegen de live Azure VM
- Verifieert routing van alle ~35 flows in `contract_flows.yaml`
- Geeft een job summary

Manueel starten: **Actions → Integration Test Suite → Run workflow**  
Je kunt daar ook één van de twee jobs skippen via de checkboxen.

---

## Wanneer hoef ik iets te updaten?

| Situatie | Wat aanpassen |
|---|---|
| Team voegt nieuwe **builder functie** toe | Één test case in `test_contract_full.py` |
| Team voegt nieuwe **routing flow** toe | Één entry in `contract_flows.yaml` |
| Team repo wordt **hernoemd** | De `repository:` regel in de workflow |

---

## Mappenstructuur

```
xml-test-env/
├── .github/workflows/
│   └── xml-integration-test.yml  — CI/CD workflow (2 jobs)
├── examples/                      — Voorbeeld XML bestanden per message type
├── schemas/                       — XSD referentie (team schemas zijn authoritatief)
├── contract_flows.yaml            — Centrale registry van alle message flows
├── test_contract_full.py          — Builder compliance test
├── test_integration.py            — VM routing test
├── check_rabbit.py                — RabbitMQ connectivity check
└── setup_queues.py                — Queue/exchange setup utility
```

---

*Groep 1 — Integratie Project 2026*
