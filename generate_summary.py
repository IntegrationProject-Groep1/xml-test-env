import os
import re
from pathlib import Path

def generate_summary():
    summary_file = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_file:
        print("Not running in GitHub Actions, skipping summary.")
        return

    output_path = Path("test-output.txt")
    if not output_path.exists():
        with open(summary_file, "a") as f:
            f.write("## ⚠️ Geen test resultaten gevonden\n")
            f.write("Het `test-output.txt` bestand ontbreekt.\n")
        return

    content = output_path.read_text()
    
    # Parse results using regex
    # Example line: [VALID] new_registration -> Q:crm.incoming EX: RK: -> SENT
    results = re.findall(r"\[(VALID|INVALID)\] (.*?) -> (.*)", content)
    
    total = len(results)
    valid_count = sum(1 for r in results if r[0] == "VALID")
    invalid_count = total - valid_count
    
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
        
        f.write("### 📝 Contract Details\n")
        f.write("| Status | Contract Naam | Bestemming | Details |\n")
        f.write("|---|---|---|---|\n")
        
        for status, name, details in results:
            status_icon = "✅" if status == "VALID" else "❌"
            
            # Split details to separate destination and send status
            dest_parts = details.split(" -> ")
            destination = dest_parts[0] if len(dest_parts) > 0 else "Onbekend"
            send_status = dest_parts[1] if len(dest_parts) > 1 else ""
            
            f.write(f"| {status_icon} | `{name}` | `{destination}` | {send_status} |\n")
        
        f.write("\n### 📨 Bericht Logboek (Live Simulatie)\n")
        f.write("> Zie de 'Run XML Contract Integration Tests' stap voor de volledige technische details.\n")
        
        # Check for summary line at the end
        summary_line = re.search(r"Summary: (.*)", content)
        if summary_line:
            f.write(f"\n**Samenvatting:** {summary_line.group(1)}\n")

if __name__ == "__main__":
    generate_summary()
