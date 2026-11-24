import sqlite3

conn = sqlite3.connect(':memory:')
schema = open('core/schema.sql', encoding='utf-8').read()
statements = [s.strip() for s in schema.split(';') if s.strip()]

for idx, stmt in enumerate(statements):
    try:
        conn.executescript(stmt + ';')
    except Exception as e:
        print(f"Error in statement {idx+1}: {e}")
        print("Statement snippet:", stmt[:300])
        break
else:
    print("All statements executed successfully!")
conn.close()
