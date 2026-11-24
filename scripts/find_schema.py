import re

with open('doc_text.txt', encoding='utf-8') as f:
    text = f.read()

# Look for DDL schema in document text
# Let's find where CREATE TABLE appears and extract until the end of views/indexes
start_pos = text.find("CREATE TABLE IF NOT EXISTS dom_warehouse_stocks")
if start_pos == -1:
    start_pos = text.find("CREATE TABLE")

# Or even better, let's search for "Спецификация таблиц по слоям" or similar heading and get the DDL code blocks.
# Let's write a robust finder in python.
print("Start pos:", start_pos)
