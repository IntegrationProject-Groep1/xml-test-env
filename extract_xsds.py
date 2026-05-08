import re
import os
from pathlib import Path

# Gebruik paden relatief aan het script zelf
SCRIPT_DIR = Path(__file__).parent
contract_path_str = os.getenv("CONTRACT_PATH", "xml-xsd-contract/XML_XSD_Contract_v2.3_Centralized 1.md")
contract_path = Path(contract_path_str)
xsd_dir = SCRIPT_DIR / "schemas"
xml_dir = SCRIPT_DIR / "examples"

xsd_dir.mkdir(parents=True, exist_ok=True)
xml_dir.mkdir(parents=True, exist_ok=True)

if not contract_path.exists():
    print(f"Contract file not found: {contract_path}")
    exit(1)

with open(contract_path, "r", encoding="utf-8") as f:
    content = f.read()

# Pattern to find ### <id> `message_type`
# We split by '---' to ensure we stay within one section
sections = content.split("---")

count = 0
for section_text in sections:
    # Match header: ### 9.1 `mailing_status`
    header_match = re.search(r"### (\d+(?:\.\d+)?) `([^`]+)`", section_text)
    if not header_match:
        continue
        
    section_id = header_match.group(1).strip()
    msg_type = header_match.group(2).strip()
    
    # Extract XSD
    xsd_match = re.search(r"#### XSD.*?```xml\s+(.*?)\s+```", section_text, re.DOTALL)
    # Extract Example XML
    xml_match = re.search(r"#### Voorbeeld XML.*?```xml\s+(.*?)\s+```", section_text, re.DOTALL)
    
    if xsd_match and xml_match:
        xsd_content = xsd_match.group(1).strip()
        xml_content = xml_match.group(1).strip()
        
        # Clean up msg_type (remove optional context in parens)
        msg_type_clean = msg_type.split()[0].replace("`", "")
        
        # Determine source from the example XML first. Some shared schemas
        # allow multiple source values, so the first XSD enumeration is not
        # necessarily the source used by this contract example.
        source_match = re.search(r"<source>([^<]+)</source>", xml_content)
        if source_match:
            source = source_match.group(1)
            base_name = f"{msg_type_clean}_{source}"
        else:
            base_name = f"{msg_type_clean}_{section_id.replace('.', '_')}"
            
        with open(xsd_dir / f"{base_name}.xsd", "w", encoding="utf-8") as f:
            f.write(xsd_content)
        with open(xml_dir / f"{base_name}.xml", "w", encoding="utf-8") as f:
            f.write(xml_content)
        
        # Generic for compatibility
        with open(xsd_dir / f"{msg_type_clean}.xsd", "w", encoding="utf-8") as f:
            f.write(xsd_content)
            
        print(f"Extracted: {base_name}")
        count += 1

print(f"Total extracted with examples: {count}")
