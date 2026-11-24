import re

with open('doc_text.txt', encoding='utf-8') as f:
    text = f.read()

# Let's extract the DDL definitions properly from the text or construct them cleanly.
# Actually, we can write out the exact schema statements in Python or fix the split words.
# Let's see what words got split across lines: DEFA ULT -> DEFAULT, etc.
# Even better, let's write out the complete, canonical schema.sql explicitly without relying on raw docx text splitting.
