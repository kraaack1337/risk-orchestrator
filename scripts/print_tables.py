with open('doc_text.txt', encoding='utf-8') as f:
    text = f.read()

import re
matches = [m.start() for m in re.finditer(r'CREATE TABLE', text)]
for idx, s in enumerate(matches):
    sub = text[s:s+1500]
    semi = sub.find(';')
    if semi != -1:
        print(f"-- TABLE {idx+1}")
        print(sub[:semi+1].strip())
        print()
