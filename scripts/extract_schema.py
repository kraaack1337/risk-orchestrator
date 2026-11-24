import re

with open('doc_text.txt', encoding='utf-8') as f:
    text = f.read()

# Let's search for DDL sections in doc_text.txt
# Find where CREATE TABLE / INDEX / VIEW statements are located and print full DDL block
ddl_block_start = text.find("CREATE TABLE")
if ddl_block_start != -1:
    print("Found DDL block around position", ddl_block_start)

# Let's extract all CREATE statements
statements = []
lines = text.splitlines()
current_stmt = []
capturing = False

for line in lines:
    stripped = line.strip()
    if stripped.startswith("CREATE TABLE") or stripped.startswith("CREATE INDEX") or stripped.startswith("CREATE VIEW"):
        if current_stmt:
            statements.append("\n".join(current_stmt))
            current_stmt = []
        capturing = True
    
    if capturing:
        current_stmt.append(line)
        if stripped.endswith(";") and not (stripped.startswith("SELECT") or stripped.startswith("FROM") or stripped.startswith("WHERE") or stripped.startswith("GROUP BY") or stripped.startswith("ORDER BY") or stripped.startswith("CASE") or stripped.startswith("WHEN") or stripped.startswith("JOIN") or stripped.startswith("ON")):
            # Check if this ';' finishes a statement
            # For views, ';' might be at the end of SELECT query
            # So if statement has SELECT, check if ';' closes the view
            full_str = "\n".join(current_stmt)
            open_parens = full_str.count("(") - full_str.count(")")
            if open_parens <= 0:
                statements.append(full_str)
                current_stmt = []
                capturing = False

if current_stmt:
    statements.append("\n".join(current_stmt))

print(f"Extracted {len(statements)} statements.")
with open('extracted_ddl.sql', 'w', encoding='utf-8') as f:
    for stmt in statements:
        f.write(stmt.strip() + "\n\n")
