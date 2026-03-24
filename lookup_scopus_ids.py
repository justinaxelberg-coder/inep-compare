"""One-off script to resolve Scopus Affiliation IDs for spotlight institutions."""
import os, sys
sys.path.insert(0, ".")
from connectors.api.scopus import ScopusConnector

c = ScopusConnector(api_key=os.environ["SCOPUS_API_KEY"])

institutions = [
    ("UFABC",    "Universidade Federal do ABC"),
    ("UNIFESP",  "Universidade Federal de Sao Paulo"),
    ("UFPA",     "Universidade Federal do Para"),
    ("IFSP",     "Instituto Federal de Educacao Ciencia e Tecnologia de Sao Paulo"),
    ("PUC-Camp", "Pontificia Universidade Catolica de Campinas"),
]

for short, name in institutions:
    hits = c.get_affiliation_id(name)
    print(f"\n{short}:")
    for h in hits:
        print(f"  {h['id']}  {h['name']}  ({h['city']})  docs={h['doc_count']}")
