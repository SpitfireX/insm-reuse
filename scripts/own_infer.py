import sqlite3
import pickle
import json
import multiprocessing
import argparse
import time

from pathlib import Path
from itertools import islice
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy

from ngram_similarity import NgramSimilarity
from modified_ngram_similarity import ModifiedNgramSimilarity


def chunk(length, iterator):
    chunk = []
    for e in iterator:
        if len(chunk) < length:
            chunk.append(e)
        else:
            yield chunk
            chunk = [e]
    if len(chunk) > 0:
        yield chunk


def label_rows(rows, use_mngs=False):
    scores = [scorer.score_texts(texta, textb) for rownum, ida, idb, texta, textb in rows]

    if use_mngs:
        scores2 = [mngs_scorer.score_texts(texta, textb) for rownum, ida, idb, texta, textb in rows]
        scores = [{**a, **b} for a, b in zip(scores, scores2)]

    rawscores = [list(s.values()) for s in scores]
    labels = model.predict(rawscores)
    return [(ida, idb, json.dumps(score), str(label)) for (_, ida, idb, _, _), score, label in zip(rows, scores, labels)]


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-o", default=Path("predictions.db"), dest="outdb",
        help="Path to the output SQLite DB", type=Path)
    argparser.add_argument("-i", dest="indb", required=True,
        help="Path to the input SQLite DB", type=Path)
    argparser.add_argument("-m", dest="modelpath", required=True,
        help="Path to the pickled ScikitLearn model to use", type=Path)
    argparser.add_argument("-t", default=multiprocessing.cpu_count(), type=int, dest="njobs",
        help="Sets the number of parallell processes to use. Defaults to all available")
    argparser.add_argument("-s", type=int, dest="start",
        help="Start of range for DB rows to process (inclusive)")
    argparser.add_argument("-e", type=int, dest="end",
        help="End of range for DB rows to process (exclusive)")
    argparser.add_argument("--mngs", default=False, action='store_true', dest="mngs",
        help="Flag for using modified ngram similarity")
    
    args = argparser.parse_args()

    incon = sqlite3.connect(args.indb)
    incur = incon.cursor()

    outcon = sqlite3.connect(args.outdb)
    outcur = outcon.cursor()
    setupsql = """
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
    outcur.execute(setupsql)
    
    if args.start or args.end:
        frags = []

        if args.start:
            frags.append(f"row >= {args.start}")
        
        if args.end:
            frags.append(f"row < {args.end}")

        sqlwhere = f"WHERE {' AND '.join(frags)}"
    else:
        sqlwhere = ""

    sqlselect = f"""
    SELECT * FROM (
        SELECT 
            ROW_NUMBER () OVER () row,
            sources.id,
            texts.id,
            sources.text,
            texts.text
        FROM
            sources
        CROSS JOIN
            texts
    )
    {sqlwhere};
    """

    rowiter = incur.execute(sqlselect)

    print(sqlselect)

    global scorer
    global model
    global mngs_scorer

    if args.mngs:
         mngs_scorer = ModifiedNgramSimilarity("de")
    else:
        mngs_scorer = None

    scorer = NgramSimilarity("de")

    with args.modelpath.open(mode="rb") as f:
        model = pickle.load(f)

    st = time.time()

    for num, batch in enumerate(chunk(args.njobs, chunk(1000, rowiter)), 1):
        with ProcessPoolExecutor(max_workers = args.njobs) as executor:
            print(f"Processing batch {num} after {time.time()-st} s")

            bst = time.time()
            jobs = []

            for workbundle in batch:
                print(f"\tSubmitting workbundle starting with row {workbundle[0][0]}")
                
                job = executor.submit(label_rows, workbundle, args.mngs)
                jobs.append(job)

            bundleresults = [f.result() for f in as_completed(jobs)]
            for results in bundleresults:
                outcur.executemany("INSERT OR IGNORE INTO predictions VALUES(?, ?, ?, ?)", results);
                outcon.commit()

            print(f"Processed batch {num} in {time.time() - bst} s")

    incon.close()

    outcon.close()

    print(f"Completed in {time.time() - st} s")

if __name__ == "__main__":
    main()