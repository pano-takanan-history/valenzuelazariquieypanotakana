import csv
from collections import defaultdict
import re
from lingpy import Wordlist
from pysem.glosses import to_concepticon


wl = Wordlist("raw/data.tsv")

proto_concepts = [[
    "GLOSS",
    "CONCEPTICON_ID",
    "CONCEPTICON_GLOSS",
    "PROTO_ID",
    "PROTO_CONCEPT"
    ]]

other_concepts = [[
    "GLOSS",
    "PROTO_ID",
    "PROTO_CONCEPT"
    ]]
concept_exists = []

concept_count = 0
concept_ids = defaultdict()

for i in wl:
    concept = re.sub("  ", " ", wl[i, "concept"])

    ID = wl[i, "proto_set"]
    p_concept = wl[i, "proto_concept"]
    # Proto-Concepts
    if wl[i, "doculect"] == "ProPa" or wl[i, "doculect"] == "ProTa":
        # print(wl[i])
        mapped = to_concepticon([{"gloss": concept}], language="es")

        if mapped[concept]:
            cid, cgl = mapped[concept][0][:2]
        else:
            cid, cgl = "", ""

        proto_concepts.append([
            concept, cid, cgl, ID, p_concept
        ])

    # Other concepts
    elif (concept, ID) not in concept_exists:
        other_concepts.append([
            concept, ID, p_concept
        ])

    concept_exists.append((concept, ID))

with open("etc/other_concepts.tsv", "w", encoding="utf8") as file:
    writer = csv.writer(file, delimiter="\t")
    writer.writerows(other_concepts)

with open("etc/proto_concepts.tsv", "w", encoding="utf8") as file:
    writer = csv.writer(file, delimiter="\t")
    writer.writerows(proto_concepts)