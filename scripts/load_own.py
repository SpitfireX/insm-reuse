"""
This script loads the scraped German texts in JSON format and stores them into a SQLite database.
The resulting database consists of two tables for sources (INSM publications) and texts (press articles) and a table for source-pair-label pairs.
"""

import sqlite3
import json

from pathlib import Path
from datetime import datetime

con = sqlite3.connect("own3.db")
cur = con.cursor()

sql_create = """
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    publication TEXT,
    language TEXT,
    date TEXT,
    author TEXT,
    url TEXT,
    text TEXT
);

CREATE TABLE IF NOT EXISTS texts (
    id TEXT PRIMARY KEY,
    publication TEXT,
    language TEXT,
    date TEXT,
    author TEXT,
    url TEXT,
    text TEXT
);

CREATE TABLE IF NOT EXISTS predictions (
    ida INTEGER NOT NULL,
    idb INTEGER NOT NULL,
    scores TEXT,
    label TEXT,
    FOREIGN KEY(ida) REFERENCES sources(id)
    FOREIGN KEY(idb) REFERENCES texts(id)
    PRIMARY KEY (ida, idb)
);
"""

cur.executescript(sql_create)

scraped_base = Path("../quellen/artikel/extracted/")

for p in scraped_base.rglob("*.json"):
    with p.open(encoding="utf-8") as f:
        data = json.load(f)
    
    finger = data["fingerprint"]
    date = datetime.strptime(data["date"], r"%Y-%m-%d").isoformat() if data["date"] else None
    title = data["title"]
    author = data["author"]
    text = data["raw-text"]
    publication = p.parts[3]
    url = data["source"]

    if "insm" in publication.lower(): # skip insm material from tweets for now
        continue

    if len(text) < 2500 \
        or "sport" in url \
        or "ratgeber" in url \
        or "unterhaltung" in url \
        or "lifestyle" in url \
        or "leute" in url \
        or "Jetzt weiterlesen. Mit dem passenden SPIEGEL-Abo." in text:
        continue

    table = "sources "if "insm" in publication.lower() else "texts"
    sql = "INSERT OR IGNORE INTO {} VALUES (?, ?, ?, ?, ?, ?, ?)".format(table)

    cat = [t.strip() for t in [title, text] if t]

    cur.execute(sql,
        (
            finger,
            publication,
            "de",
            date,
            author,
            url,
            "\n".join(cat).strip()
        )
    )

con.commit()

insm_base = Path("../insm_pressemeldungen/")

for p in insm_base.rglob("*.json"):
    with p.open(encoding="utf-8") as f:
        data = json.load(f)
    
    finger = data["fingerprint"]
    date = datetime.strptime(data["date"], r"%Y-%m-%d").isoformat() if data["date"] else None
    title = data["title"]
    author = data["author"]
    text = data["raw-text"]
    publication = "insm_presse"
    url = data["source"]

    if len(text) < 2500:
        continue

    sql = "INSERT OR IGNORE INTO sources VALUES (?, ?, ?, ?, ?, ?, ?)"

    cat = [t.strip() for t in [title, text] if t]

    cur.execute(sql,
        (
            finger,
            publication,
            "de",
            date,
            author,
            url,
            "\n".join(cat).strip()
        )
    )

con.commit()
con.close()
