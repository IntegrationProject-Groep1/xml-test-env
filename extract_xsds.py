import re
import os
from pathlib import Path

contract_path_str = os.getenv("CONTRACT_PATH", "xml-xsd-contract/XML_XSD_Contract_v2.3_Centralized 1.md")
contract_path = Path(contract_path_str)
xsd_dir = Path("xml-test-env/schemas")
xml_dir = Path("xml-test-env/examples")
xsd_dir.mkdir(parents=True, exist_ok=True)
xml_dir.mkdir(parents=True, exist_ok=True)

with open(contract_path, "r", encoding="utf-8") as f:
    content = f.read()

# Pattern to find ### <id> `message_type`
# followed by XSD and then Example XML
# Improved regex to handle (conform ...) in headers
pattern = r"### (\d+(?:\.\d+)?) `([^`]+)`.*?#### XSD.*?```xml\s+(.*?)\s+```.*?#### Voorbeeld XML.*?```xml\s+(.*?)\s+```"
matches = re.finditer(pattern, content, re.DOTALL)

count = 0
for match in matches:
    section = match.group(1).strip()
    msg_type = match.group(2).strip()
    xsd_content = match.group(3).strip()
    xml_content = match.group(4).strip()
    
    msg_type = msg_type.split()[0].replace("`", "")
    
    source_match = re.search(r'<xs:element name="source">.*?<xs:enumeration value="([^"]+)"/>', xsd_content, re.DOTALL)
    if source_match:
        source = source_match.group(1)
        base_name = f"{msg_type}_{source}"
    else:
        base_name = f"{msg_type}_{section.replace('.', '_')}"
        
    with open(xsd_dir / f"{base_name}.xsd", "w", encoding="utf-8") as f:
        f.write(xsd_content)
    with open(xml_dir / f"{base_name}.xml", "w", encoding="utf-8") as f:
        f.write(xml_content)
    
    # Generic for compatibility
    with open(xsd_dir / f"{msg_type}.xsd", "w", encoding="utf-8") as f:
        f.write(xsd_content)
        
    print(f"Extracted: {base_name}")
    count += 1

print(f"Total extracted with examples: {count}")
