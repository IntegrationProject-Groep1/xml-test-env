import os
import re
from pathlib import Path
import subprocess

def get_receiver_logs():
    """Haal logs op van de docker containers en filter op ontvangen berichten."""
    try:
        # Haal logs op van alle mock services
        result = subprocess.run(
            ["docker", "compose", "logs", "--no-log-prefix"], 
            capture_output=True, 
            text=True, 
            cwd=Path(__file__).parent
        )
        return result.stdout
    except Exception as e:
        print(f"Fout bij ophalen logs: {e}")
        return ""

def generate_summary():
    summary_file = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_file:
        print("Not running in GitHub Actions, skipping summary.")
        return

    output_path = Path(__file__).parent / "test-output.txt"
    if not output_path.exists():
        with open(summary_file, "a") as f:
            f.write("## ⚠️ Geen test resultaten gevonden\n")
            f.write("Het `test-output.txt` bestand ontbreekt.\n")
        return

    content = output_path.read_text()
    
    # Parse verzonden resultaten
    results = re.findall(r"\[(VALID|INVALID)\] (.*?) -> (.*)", content)
    
    total = len(results)
    valid_count = sum(1 for r in results if r[0] == "VALID")
    invalid_count = total - valid_count
    
    # Haal logs op voor ontvangst verificatie
    logs = get_receiver_logs()
    # Zoek naar patronen zoals: [CRM] >>> ONTVANGEN: new_registration
    received_matches = re.findall(r"\[(.*?)\] >>> ONTVANGEN: (.*?)$", logs, re.MULTILINE)
    
    # Maak een map van ontvangen berichten per type
    received_map = {}
    for service, msg_type in received_matches:
        if msg_type not in received_map:
            received_map[msg_type] = []
        received_map[msg_type].append(service)

    with open(summary_file, "a") as f:
        f.write("# 📂 XML Integratie & Contract Test Rapport\n\n")
        
        # Status Bar
        if total > 0:
            pct = int((valid_count / total) * 100)
            bar_filled = "█" * (pct // 5)
            bar_empty = "░" * (20 - pct // 5)
            f.write(f"### 🚀 Algemene Voortgang: {valid_count}/{total} ({pct}%)\n")
            f.write("```\n")
            f.write(f"{bar_filled}{bar_empty} {pct}%\n")
            f.write("```\n")
        
        f.write(f"✅ **Valid**: {valid_count} | ❌ **Invalid**: {invalid_count}\n\n")
        
        f.write("### 📝 Contract & Delivery Details\n")
        f.write("| Status | Contract Naam | Bestemming | Verzonden | Ontvangen door |\n")
        f.write("|---|---|---|---|---|\n")
        
        for status, name, details in results:
            status_icon = "✅" if status == "VALID" else "❌"
            
            # Split details to separate destination and send status
            dest_parts = details.split(" -> ")
            destination = dest_parts[0] if len(dest_parts) > 0 else "Onbekend"
            send_status = "✔️" if "SENT" in details else "❌"
            
            # Check of het type (onderdeel van de naam) ontvangen is
            # De naam bevat vaak het type gevolgd door source of sectie_id
            # Bijv: payment_registered_kassa -> type is payment_registered
            msg_type_guess = name.rsplit('_', 1)[0]
            receivers = received_map.get(msg_type_guess, [])
            
            # Fallback: check of de naam zelf als type is gelogd
            if not receivers:
                receivers = received_map.get(name, [])
            
            # Fallback 2: check of een deel van de naam overeenkomt (bijv voor complexe namen)
            if not receivers:
                for logged_type, svcs in received_map.items():
                    if logged_type in name:
                        receivers = svcs
                        break

            received_by = ", ".join([f"`{s}`" for s in receivers]) if receivers else "⏳ _Niet ontvangen_"
            
            f.write(f"| {status_icon} | `{name}` | `{destination}` | {send_status} | {received_by} |\n")
        
        f.write("\n### 📨 Bericht Logboek (Live Simulatie)\n")
        f.write("> De 'Ontvangen door' kolom bevestigt dat de mock-service het bericht daadwerkelijk uit de queue heeft gehaald.\n")
        
        summary_line = re.search(r"Summary: (.*)", content)
        if summary_line:
            f.write(f"\n**Samenvatting:** {summary_line.group(1)}\n")

if __name__ == "__main__":
    generate_summary()
