import re

with open('doc_text.txt', encoding='utf-8') as f:
    text = f.read()

# Let's find where the DDL block is in doc_text.txt
# We can search for "CREATE TABLE IF NOT EXISTS dom_warehouse_stocks" or similar
# Let's print out the exact statements from the text where CREATE TABLE/INDEX/VIEW occurs.
matches = list(re.finditer(r'CREATE (TABLE|INDEX|VIEW)', text))
stmts = []
for i in range(len(matches)):
    start = matches[i].start()
    end = matches[i+1].start() if i + 1 < len(matches) else start + 2000
    chunk = text[start:end]
    # Find the matching closing semicolon for the CREATE statement
    semi_idx = chunk.find(';')
    if semi_idx != -1:
        stmt = chunk[:semi_idx+1]
        # Clean up excessive internal whitespace / line breaks if needed, but keep SQL structure
        stmts.append(stmt.strip())

print(f"Extracted {len(stmts)} statements.")
with open(r'c:/Users/marko/Desktop/Orchestrator case/risk-orchestrator/core/schema.sql', 'w', encoding='utf-8') as f:
    for s in stmts:
        f.write(s + "\n\n")
print("Written core/schema.sql successfully.")
