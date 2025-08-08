import sqlite3, json
from contextlib import contextmanager

DB_PATH = "./data.sqlite"

@contextmanager
def conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("PRAGMA foreign_keys=ON")
    yield c
    c.commit()
    c.close()

def init_db():
    with open("db/schema.sql","r",encoding="utf-8") as f:
        schema = f.read()
    with conn() as c:
        c.executescript(schema)

def write_summary(crawl_id:str, domain:str, status:str, total:int, summary:dict):
    with conn() as c:
        c.execute("REPLACE INTO crawls (crawl_id,domain,started_at,finished_at,status,total_urls,summary_json) VALUES (?,?,?,?,?,?,?)",
                  (crawl_id, domain, summary.get('started_at'), summary.get('finished_at'), status, total, json.dumps(summary)))

def insert_many(table:str, rows:list):
    if not rows: return
    with conn() as c:
        cols = rows[0].keys()
        placeholders = ",".join(["?"]*len(cols))
        sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
        c.executemany(sql, [tuple(r[k] for k in cols) for r in rows])

def query_latest(domain:str):
    with conn() as c:
        cur = c.execute("SELECT crawl_id, started_at, finished_at, summary_json FROM crawls WHERE domain=? ORDER BY started_at DESC LIMIT 1",(domain,))
        row = cur.fetchone()
        if not row: return None
        crawl_id, started_at, finished_at, summary_json = row
        sm = json.loads(summary_json or '{}')
        return {"crawl_id":crawl_id, "started_at":started_at, "finished_at":finished_at, "summary_metrics": sm.get('metrics', {})}

def stream_csv(crawl_id:str, dataset:str):
    table = dataset if dataset in {"urls","redirects","broken_links","duplicates_titles","duplicates_meta","images","structured_data"} else None
    if not table: return None
    with conn() as c:
        cur = c.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
        yield ";".join(cols) + "\n"
        cur = c.execute(f"SELECT {','.join(cols)} FROM {table} WHERE crawl_id=?", (crawl_id,))
        for row in cur:
            vals = [str(v if v is not None else '') for v in row]
            yield ";".join(vals) + "\n"
