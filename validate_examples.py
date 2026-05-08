import os
from pathlib import Path
from lxml import etree

EXAMPLES_DIR = Path("xml-test-env/examples")
SCHEMAS_DIR = Path("xml-test-env/schemas")

def validate_all():
    examples = list(EXAMPLES_DIR.glob("*.xml"))
    print(f"Validating {len(examples)} examples against schemas...\n")
    
    passed = 0
    failed = 0
    
    for example_path in examples:
        base_name = example_path.stem
        # Try to find specific schema first, then generic
        schema_path = SCHEMAS_DIR / f"{base_name}.xsd"
        if not schema_path.exists():
            # Try generic name (e.g. new_registration_frontend -> new_registration)
            generic_name = base_name.rsplit('_', 1)[0]
            schema_path = SCHEMAS_DIR / f"{generic_name}.xsd"
            
        if not schema_path.exists():
            print(f"  [SKIP] {base_name}: No schema found")
            continue
            
        try:
            schema_doc = etree.parse(str(schema_path))
            schema = etree.XMLSchema(schema_doc)
            
            with open(example_path, "rb") as f:
                xml_content = f.read()
                
            doc = etree.fromstring(xml_content)
            schema.assertValid(doc)
            print(f"  [PASS] {base_name}")
            passed += 1
        except etree.DocumentInvalid as e:
            print(f"  [FAIL] {base_name}: {e}")
            failed += 1
        except Exception as e:
            print(f"  [ERROR] {base_name}: {e}")
            failed += 1
            
    print(f"\nSummary: {passed} passed, {failed} failed.")

if __name__ == "__main__":
    validate_all()
