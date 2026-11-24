import sqlite3

conn = sqlite3.connect(':memory:')
conn.execute('PRAGMA foreign_keys = ON;')
schema = open('core/schema.sql', encoding='utf-8').read()
conn.executescript(schema)
cursor = conn.cursor()
cursor.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view', 'index') AND name NOT LIKE 'sqlite_%'")
items = cursor.fetchall()
print(f"Total items: {len(items)}")
tables = [i[0] for i in items if i[1] == 'table']
views = [i[0] for i in items if i[1] == 'view']
indexes = [i[0] for i in items if i[1] == 'index']
print(f"Tables ({len(tables)}): {tables}")
print(f"Views ({len(views)}): {views}")
print(f"Indexes ({len(indexes)}): {indexes}")
conn.close()
