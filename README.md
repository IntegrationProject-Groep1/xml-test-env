# 🧪 XML Test Environment & Playground

Deze directory is de **sandbox** en het **referentie-archief** voor alle XML-berichten en XSD-schema's binnen Groep 1. Het is ontworpen om een veilige plek te bieden voor het ontwikkelen, debuggen en documenteren van het message-contract voordat wijzigingen in de hoofd-integratietest worden doorgevoerd.

---

## 🎯 Waarom deze map?
In een complex project met 8 teams is het contract (de XSD) heilig. Deze map helpt je om:
1.  **Contract-First Design:** Ontwerp je bericht (XML) en regel (XSD) hier als eerste.
2.  **Referentie:** Nieuwe teamleden kunnen hier precies zien hoe een `payment_registered` of `session_created` bericht eruit moet zien.
3.  **Local Development:** Test je eigen service tegen een lokale RabbitMQ met mock-receivers zonder dat de hele infra omhoog moet.

---

## 📂 Structuur van de Playground

| Map/Bestand | Omschrijving |
| :--- | :--- |
| `schemas/` | Bevat alle `.xsd` bestanden. Dit is de "Source of Truth" voor berichtstructuren. |
| `examples/` | Voorbeeld `.xml` bestanden voor elk berichttype. Handig voor documentatie en testen. |
| `senders/` | Python scripts die specifieke berichten kunnen genereren en versturen. |
| `receivers/` | Mock-diensten die luisteren naar queues en loggen wat ze ontvangen (gebruikt in Docker). |
| `docker-compose.yml` | Start een lokale RabbitMQ en alle mock-receivers voor een volledige sandbox ervaring. |

---

## 🛠️ Beschikbare Tools (Scripts)

### 1. `validate_examples.py`
**Gebruik:** `python validate_examples.py`
Valideert alle XML-bestanden in de `examples/` map tegen de bijbehorende XSD in `schemas/`. Dit is de eerste stap bij elke wijziging.

### 2. `run_all.py`
**Gebruik:** `python run_all.py [--dry-run]`
Verstuurt een hele batterij aan testberichten (gedefinieerd in de `senders/`) naar de lokale RabbitMQ. 
*   Gebruik `--dry-run` om alleen de XSD-validatie te doen zonder te versturen.

### 3. `setup_queues.py`
**Gebruik:** `python setup_queues.py`
Configureert een schone RabbitMQ-instantie met alle benodigde exchanges (topic/fanout) en queues zoals gedefinieerd in het v2.3 contract.

### 4. `run_contract_tests.py`
Dit script bevat de mapping van `(source, type)` naar de juiste RabbitMQ bestemming. Het is de motor achter de automatische validatietesten in deze map.

### 5. `extract_xsds.py`
Een handige utility om XSD-definities uit documentatie of andere bronnen te trekken en op te slaan in de `schemas/` map.

---

## 🚀 Aan de slag

### Stap 1: De Sandbox starten (Docker)
Als je wilt zien hoe berichten echt door queues vloeien:
```bash
docker-compose up -d
```
Dit start RabbitMQ en 8 "receiver" containers die elk bericht dat ze ontvangen loggen.

### Stap 2: Queues instellen
```bash
python setup_queues.py
```

### Stap 3: Testberichten sturen
```bash
python run_all.py
```
Check daarna de logs van je Docker containers om te zien of de berichten zijn aangekomen:
```bash
docker-compose logs -f
```

---

## ⚖️ Wanneer gebruik ik wat?

| Scenario | Gebruik `xml-test-env` | Gebruik `Infra/scripts/test_integration.py` |
| :--- | :---: | :---: |
| Een nieuw berichttype toevoegen | ✅ | ❌ |
| Een XSD fout opsporen | ✅ | ❌ |
| Documentatie zoeken van een bericht | ✅ | ❌ |
| Controleren of de routing in productie klopt | ❌ | ✅ |
| Een end-to-end integratie test draaien | ❌ | ✅ |
| CI/CD Pipeline validatie | ❌ | ✅ |

---

## 📝 Workflow voor Nieuwe Integraties
1.  **Definieer:** Maak je `.xsd` in `schemas/`.
2.  **Voorbeeld:** Maak een `.xml` in `examples/`.
3.  **Check:** Draai `python validate_examples.py`.
4.  **Implementeer:** Voeg een functie toe in `senders/` om dit bericht te versturen.
5.  **Verifieer:** Draai de sandbox (Docker) en check of de mock-receiver het bericht pakt.
6.  **Commit:** Zodra het werkt, update je ook de centrale suite in `Infra/scripts/`.

---
*Groep 1 — Integratie Project 2026*
