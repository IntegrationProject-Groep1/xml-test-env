# XML Test Environment & Playground

Deze directory dient als een **ontwikkelomgeving en naslagwerk** voor het XML/XSD contract van Groep 1.

## Doel van deze omgeving
In tegenstelling tot de geautomatiseerde integratietests, is deze map bedoeld voor:
1.  **Prototyping:** Snel testen van nieuwe berichtstructuren of wijzigingen in het contract voordat ze in de hoofdcode worden opgenomen.
2.  **Referentie:** Een centrale plek voor alle `.xsd` schema's en `.xml` voorbeelden (zie de mappen `schemas/` en `examples/`).
3.  **Handmatige Validatie:** Gebruik `validate_examples.py` om te controleren of handmatig gemaakte XML-bestanden voldoen aan de standaarden.
4.  **Sandbox:** Experimenteren met RabbitMQ-ontvangers (`receivers/`) en zenders (`senders/`) in een geïsoleerde omgeving via de bijgevoegde `docker-compose.yml`.

## xml-test-env vs. Infra/scripts
Er is een belangrijk verschil tussen deze omgeving en de scripts in de `Infra` map:

| Kenmerk | `xml-test-env` (Deze map) | `Infra/scripts/test_integration.py` |
| :--- | :--- | :--- |
| **Primaire Focus** | Ontwikkeling & Referentie | Productie-waardige Integratietests |
| **Routing Controle** | Beperkt / Handmatig | Volledige end-to-end flow verificatie |
| **XSD Bron** | Losse `.xsd` bestanden | Ingebouwde (gecompileerde) schema's |
| **Gebruiksscenario** | "Ik wil een nieuw berichttype ontwerpen" | "Ik wil bewijzen dat het systeem werkt" |

## Hoe te gebruiken voor nieuwe integraties
Als je een nieuwe message flow wilt toevoegen aan het systeem:
1.  Maak een nieuw `.xsd` schema aan in `schemas/`.
2.  Maak een voorbeeld `.xml` aan in `examples/`.
3.  Draai `python validate_examples.py` om te zien of ze matchen.
4.  Zodra je tevreden bent, verplaats je de logica naar de centrale integratietest in `Infra/scripts/test_integration.py` voor permanente monitoring en CI/CD.

---
*Groep 1 — Integratie Project 2026*
