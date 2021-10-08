"""
This script loads the TEI-Version of the METER corpus and transforms it into a SQLite database.
The resulting database consists of two tables for sources and texts and two tables for source-text-pairs with METER labels.
"""

import sqlite3
import io

from pathlib import Path
from datetime import datetime

from bs4 import BeautifulSoup

con = sqlite3.connect("meter.db")

sql_create = """
CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY,
    publication TEXT,
    language TEXT,
    date TEXT,
    category TEXT,
    catchline TEXT,
    text TEXT
);

CREATE TABLE IF NOT EXISTS texts (
    id INTEGER PRIMARY KEY,
    publication TEXT,
    language TEXT,
    date TEXT,
    category TEXT,
    catchline TEXT,
    text TEXT
);

CREATE TABLE IF NOT EXISTS ground_truth (
    ida INTEGER NOT NULL,
    idb INTEGER NOT NULL,
    label TEXT,
    FOREIGN KEY(ida) REFERENCES sources(id)
    FOREIGN KEY(idb) REFERENCES texts(id)
    PRIMARY KEY (ida, idb)
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

dtd = b"""
<!ENTITY half	"&#x00BD;"> <!-- VULGAR FRACTION ONE HALF -->
<!ENTITY frac12	"&#x00BD;"> <!-- VULGAR FRACTION ONE HALF -->
<!ENTITY frac14	"&#x00BC;"> <!-- VULGAR FRACTION ONE QUARTER -->
<!ENTITY frac34	"&#x00BE;"> <!-- VULGAR FRACTION THREE QUARTERS -->
<!ENTITY frac18	"&#x215B;"> <!--  -->
<!ENTITY frac38	"&#x215C;"> <!--  -->
<!ENTITY frac58	"&#x215D;"> <!--  -->
<!ENTITY frac78	"&#x215E;"> <!--  -->
<!ENTITY sup1	"&#x00B9;"> <!-- SUPERSCRIPT ONE -->
<!ENTITY sup2	"&#x00B2;"> <!-- SUPERSCRIPT TWO -->
<!ENTITY sup3	"&#x00B3;"> <!-- SUPERSCRIPT THREE -->
<!ENTITY plus	"&#x002B;"> <!-- PLUS SIGN -->
<!ENTITY plusmn	"&#x00B1;"> <!-- PLUS-MINUS SIGN -->
<!ENTITY lt	"&#38;#60;"> <!-- LESS-THAN SIGN -->
<!ENTITY equals	"&#x003D;"> <!-- EQUALS SIGN -->
<!ENTITY gt	"&#x003E;"> <!-- GREATER-THAN SIGN -->
<!ENTITY divide	"&#x00F7;"> <!-- DIVISION SIGN -->
<!ENTITY times	"&#x00D7;"> <!-- MULTIPLICATION SIGN -->
<!ENTITY curren	"&#x00A4;"> <!-- CURRENCY SIGN -->
<!ENTITY pound	"&#x00A3;"> <!-- POUND SIGN -->
<!ENTITY dollar	"&#x0024;"> <!-- DOLLAR SIGN -->
<!ENTITY cent	"&#x00A2;"> <!-- CENT SIGN -->
<!ENTITY yen	"&#x00A5;"> <!-- YEN SIGN -->
<!ENTITY num	"&#x0023;"> <!-- NUMBER SIGN -->
<!ENTITY percnt	"&#x0025;"> <!-- PERCENT SIGN -->
<!ENTITY amp	"&#38;#38;"> <!-- AMPERSAND -->
<!ENTITY ast	"&#x002A;"> <!-- ASTERISK OPERATOR -->
<!ENTITY commat	"&#x0040;"> <!-- COMMERCIAL AT -->
<!ENTITY lsqb	"&#x005B;"> <!-- LEFT SQUARE BRACKET -->
<!ENTITY bsol	"&#x005C;"> <!-- REVERSE SOLIDUS -->
<!ENTITY rsqb	"&#x005D;"> <!-- RIGHT SQUARE BRACKET -->
<!ENTITY lcub	"&#x007B;"> <!-- LEFT CURLY BRACKET -->
<!ENTITY horbar	"&#x2015;"> <!-- HORIZONTAL BAR -->
<!ENTITY verbar	"&#x007C;"> <!-- VERTICAL LINE -->
<!ENTITY rcub	"&#x007D;"> <!-- RIGHT CURLY BRACKET -->
<!ENTITY micro	"&#x00B5;"> <!-- MICRO SIGN -->
<!ENTITY ohm	"&#x2126;"> <!-- OHM SIGN -->
<!ENTITY deg	"&#x00B0;"> <!-- DEGREE SIGN -->
<!ENTITY ordm	"&#x00BA;"> <!-- MASCULINE ORDINAL INDICATOR -->
<!ENTITY ordf	"&#x00AA;"> <!-- FEMININE ORDINAL INDICATOR -->
<!ENTITY sect	"&#x00A7;"> <!-- SECTION SIGN -->
<!ENTITY para	"&#x00B6;"> <!-- PILCROW SIGN -->
<!ENTITY middot	"&#x00B7;"> <!-- MIDDLE DOT -->
<!ENTITY larr	"&#x2190;"> <!-- LEFTWARDS DOUBLE ARROW -->
<!ENTITY rarr	"&#x2192;"> <!-- RIGHTWARDS DOUBLE ARROW -->
<!ENTITY uarr	"&#x2191;"> <!-- UPWARDS ARROW -->
<!ENTITY darr	"&#x2193;"> <!-- DOWNWARDS ARROW -->
<!ENTITY copy	"&#x00A9;"> <!-- COPYRIGHT SIGN -->
<!ENTITY reg	"&#x00AE;"> <!-- REG TRADE MARK SIGN -->
<!ENTITY trade	"&#x2122;"> <!-- TRADE MARK SIGN -->
<!ENTITY brvbar	"&#x00A6;"> <!-- BROKEN BAR -->
<!ENTITY not	"&#x00AC;"> <!-- NOT SIGN -->
<!ENTITY sung	"&#x2669;"> <!--  -->
<!ENTITY excl	"&#x0021;"> <!-- EXCLAMATION MARK -->
<!ENTITY iexcl	"&#x00A1;"> <!-- INVERTED EXCLAMATION MARK -->
<!ENTITY quot	"&#x0022;"> <!-- QUOTATION MARK -->
<!ENTITY apos	"&#x0027;"> <!-- APOSTROPHE -->
<!ENTITY lpar	"&#x0028;"> <!-- LEFT PARENTHESIS -->
<!ENTITY rpar	"&#x0029;"> <!-- RIGHT PARENTHESIS -->
<!ENTITY comma	"&#x002C;"> <!-- COMMA -->
<!ENTITY lowbar	"&#x005F;"> <!-- LOW LINE -->
<!ENTITY hyphen	"&#x002D;"> <!-- HYPHEN-MINUS -->
<!ENTITY period	"&#x002E;"> <!-- FULL STOP -->
<!ENTITY sol	"&#x002F;"> <!-- SOLIDUS -->
<!ENTITY colon	"&#x003A;"> <!-- COLON -->
<!ENTITY semi	"&#x003B;"> <!-- SEMICOLON -->
<!ENTITY quest	"&#x003F;"> <!-- QUESTION MARK -->
<!ENTITY iquest	"&#x00BF;"> <!-- INVERTED QUESTION MARK -->
<!ENTITY laquo	"&#x00AB;"> <!-- LEFT-POINTING DOUBLE ANGLE QUOTATION MARK -->
<!ENTITY raquo	"&#x00BB;"> <!-- RIGHT-POINTING DOUBLE ANGLE QUOTATION MARK -->
<!ENTITY lsquo	"&#x2018;"> <!--  -->
<!ENTITY rsquo	"&#x2019;"> <!-- RIGHT SINGLE QUOTATION MARK -->
<!ENTITY ldquo	"&#x201C;"> <!--  -->
<!ENTITY rdquo	"&#x201D;"> <!-- RIGHT DOUBLE QUOTATION MARK -->
<!ENTITY nbsp	"&#x00A0;"> <!-- NO-BREAK SPACE -->
<!ENTITY shy	"&#x00AD;"> <!-- SOFT HYPHEN -->
"""

cur = con.cursor()
cur.executescript(sql_create)

def clean_text(text):
    text = text.replace("", "£")
    return text

meter_base = Path("../ressourcen/METER/meter_corpus")

src_id = 0
txt_id = 0

for p in meter_base.iterdir():
    if p.match("meter*_*_*.xml"):
        with p.open(mode="rb") as f:
            buf = f.read()

        soup = BeautifulSoup(io.BytesIO(dtd + buf), "lxml")

        texts = soup.find_all("text", {"n": True})
        
        for text in texts:
            # catchline for grouping similar stories
            catchline = text["n"]

            # first, gather all source texts into one
            sources = text.find_all("div", {"ana": "src"})

            # extract all the relevant attributes
            stype = sources[0]["type"]
            spub, sdate, _ = sources[0]["n"].split('-')
            sdate = datetime.strptime(sdate, "%d%m%Y")  
            stext = "\n".join(s.text for s in sources).strip()

            cur.execute(
                "INSERT OR IGNORE INTO sources VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    src_id,
                    spub,
                    "en",
                    sdate.isoformat(),
                    stype,
                    catchline,
                    clean_text(stext)
                )
            )

            # then process all the press texts

            texts = text.find_all("div", {"ana": ["wd", "pd", "nd"]})

            for text in texts:
                # first, insert the text into the text table
                # with the same attributes as the source
                ttype = text["type"]
                tpub, tdate, _ = text["n"].split('-')
                tdate = datetime.strptime(tdate, "%d%m%Y")  
                ttext = text.text.strip()
                tlabel = text["ana"] # additional label for relation

                cur.execute(
                    "INSERT OR IGNORE INTO texts VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        txt_id,
                        tpub,
                        "en",
                        tdate.isoformat(),
                        ttype,
                        catchline,
                        clean_text(ttext)
                    )
                )

                # second, add a source -> text relation to the ground_truth table
                cur.execute(
                    "INSERT OR IGNORE INTO ground_truth VALUES (?, ?, ?)",
                    (
                        src_id,
                        txt_id,
                        tlabel
                    )
                )

                txt_id += 1
            src_id += 1

con.commit()
con.close()
