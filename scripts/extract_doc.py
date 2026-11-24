import zipfile
import xml.etree.ElementTree as ET

docx_path = r"C:\Users\marko\Desktop\Orchestrator case\Аудит ТЗ и Стека Оркестратора.docx"
out_path = r"C:\Users\marko\Desktop\Orchestrator case\risk-orchestrator\doc_text.txt"

with zipfile.ZipFile(docx_path) as z:
    xml_content = z.read('word/document.xml')
    tree = ET.fromstring(xml_content)
    texts = [e.text for e in tree.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}t') if e.text]
    full_text = '\n'.join(texts)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(full_text)
print("Extracted successfully, length:", len(full_text))
