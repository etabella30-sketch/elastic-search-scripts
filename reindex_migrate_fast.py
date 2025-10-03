import os
import sys
import json
import time
import argparse
import warnings
from datetime import datetime, timezone
from typing import Dict, Tuple, Optional, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, ElasticsearchWarning
from elasticsearch.exceptions import AuthenticationException, NotFoundError, BadRequestError
from elasticsearch.helpers import scan, parallel_bulk

import psycopg2
import psycopg2.extras

# -------------------- ENV & CLIENTS --------------------

def load_env():
    load_dotenv()
    es_hosts = os.getenv("ELASTICSEARCH_HOSTS")
    es_user  = os.getenv("ELASTICSEARCH_USER")
    es_pass  = os.getenv("ELASTICSEARCH_PASSWORD")
    if not all([es_hosts, es_user, es_pass]):
        print("Missing ES env vars: ELASTICSEARCH_HOSTS, ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD", file=sys.stderr)
        sys.exit(2)

    pg_host = os.getenv("POSTGRES_HOST")
    pg_port = int(os.getenv("POSTGRES_PORT", "5432"))
    pg_user = os.getenv("POSTGRES_USER")
    pg_pass = os.getenv("POSTGRES_PASSWORD")
    pg_db   = os.getenv("POSTGRES_DB")
    sql_file = os.getenv("POSTGRES_SQL_FILE")
    sql_text = os.getenv("POSTGRES_SQL")
    if not all([pg_host, pg_user, pg_pass, pg_db]):
        print("Missing Postgres env vars: POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB", file=sys.stderr)
        sys.exit(2)
    if not (sql_file or sql_text):
        print("Provide SQL via POSTGRES_SQL_FILE or POSTGRES_SQL in .env", file=sys.stderr)
        sys.exit(2)
    if sql_file and not os.path.exists(sql_file):
        print(f"POSTGRES_SQL_FILE not found: {sql_file}", file=sys.stderr)
        sys.exit(2)

    return {
        "es": {"hosts": es_hosts, "user": es_user, "password": es_pass},
        "pg": {"host": pg_host, "port": pg_port, "user": pg_user, "password": pg_pass,
               "database": pg_db, "sql_file": sql_file, "sql_text": sql_text}
    }

def es_client(es_conf) -> Elasticsearch:
    warnings.simplefilter("ignore", ElasticsearchWarning)
    # Retry and compress to reduce overhead
    return Elasticsearch(
        hosts=[es_conf["hosts"]],
        basic_auth=(es_conf["user"], es_conf["password"]),
        verify_certs=False,
        request_timeout=60,
        retry_on_timeout=True,
        max_retries=5,
        http_compress=True
    )

# -------------------- ES MAPPING/SETTINGS BUILDERS --------------------

def fetch_source_analysis_and_ct(es: Elasticsearch, source_index: str):
    m = es.indices.get_mapping(index=source_index)
    mappings = getattr(m, "body", m)[source_index]["mappings"]
    props = mappings.get("properties", {})
    ct_mapping = props.get("ct")
    if ct_mapping is None:
        raise RuntimeError("Source mapping must contain a 'ct' field.")
    s = es.indices.get_settings(index=source_index, flat_settings=False, include_defaults=False)
    settings_full = getattr(s, "body", s)[source_index]["settings"]
    analysis = settings_full.get("index", {}).get("analysis", {})
    return analysis, ct_mapping

def build_destination_mapping(ct_mapping: dict) -> dict:
    return {
        "properties": {
            "o_bdid": {"type": "integer"},
            "o_bid":  {"type": "integer"},
            "o_cid":  {"type": "integer"},
            "o_sid":  {"type": "integer"},
            "bdid": {"type": "keyword"},
            "bid":  {"type": "keyword"},
            "cid":  {"type": "keyword"},
            "sid":  {"type": "keyword"},
            "ct":   ct_mapping,
            "pg":   {"type": "integer"},
        }
    }

def build_destination_settings(analysis: dict, shards: Optional[int], replicas: Optional[int]) -> dict:
    settings = {}
    if analysis:
        settings["analysis"] = analysis
    settings.setdefault("index", {})
    if shards is not None:
        settings["index"]["number_of_shards"] = str(shards)
    if replicas is not None:
        settings["index"]["number_of_replicas"] = str(replicas)
    return settings

def ensure_dest_index(es: Elasticsearch, dest_index: str, mappings: dict, settings: dict, force: bool):
    exists = es.indices.exists(index=dest_index).body
    if exists and not force:
        print(f"Destination index '{dest_index}' already exists. Use --force to reuse it.", file=sys.stderr)
        sys.exit(1)
    if not exists:
        es.indices.create(index=dest_index, mappings=mappings, settings=settings)
        print(f"Created destination index '{dest_index}'")
    else:
        print(f"Reusing existing destination index '{dest_index}' (--force)")

def optimize_dest_index(es: Elasticsearch, dest_index: str, enable: bool):
    """Temporarily set replicas=0 and refresh_interval=-1 for faster writes, then restore."""
    if enable:
        es.indices.put_settings(
            index=dest_index,
            settings={"index": {"number_of_replicas": "0", "refresh_interval": "-1"}}
        )
    else:
        # restore to defaults (replicas left as-is; user can adjust)
        es.indices.put_settings(
            index=dest_index,
            settings={"index": {"refresh_interval": "1s"}}
        )

# -------------------- POSTGRES LOADER --------------------

def read_sql(pg_conf: dict) -> str:
    if pg_conf["sql_file"]:
        with open(pg_conf["sql_file"], "r", encoding="utf-8") as f:
            return f.read()
    return pg_conf["sql_text"]

def load_pg_mappings(pg_conf: dict) -> Tuple[Dict[int, str], Dict[int, str], Dict[int, str], Dict[int, str]]:
    sql = read_sql(pg_conf)
    conn = psycopg2.connect(
        host=pg_conf["host"], port=pg_conf["port"],
        user=pg_conf["user"], password=pg_conf["password"],
        dbname=pg_conf["database"]
    )
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)

    map_bdid: Dict[int, str] = {}
    map_bid:  Dict[int, str] = {}
    map_cid:  Dict[int, str] = {}
    map_sid:  Dict[int, str] = {}

    rows = cur.fetchall()
    def to_int(v):
        try:
            return int(v) if v is not None and str(v).strip() != "" else None
        except Exception:
            return None
    for r in rows:
        old_bdid = to_int(r.get("o_bdid"))
        old_bid  = to_int(r.get("o_bid"))
        old_cid  = to_int(r.get("o_cid"))
        old_sid  = to_int(r.get("o_sid"))
        new_bdid = r.get("bdid")
        new_bid  = r.get("bid")
        new_cid  = r.get("cid")
        new_sid  = r.get("sid")
        if old_bdid is not None and new_bdid is not None: map_bdid[old_bdid] = str(new_bdid)
        if old_bid  is not None and new_bid  is not None: map_bid[old_bid]   = str(new_bid)
        if old_cid  is not None and new_cid  is not None: map_cid[old_cid]   = str(new_cid)
        if old_sid  is not None and new_sid  is not None: map_sid[old_sid]   = str(new_sid)

    cur.close()
    conn.close()
    print(f"Loaded {len(rows)} mapping rows from Postgres.")
    print(f"Distinct mappings — bdid:{len(map_bdid)} bid:{len(map_bid)} cid:{len(map_cid)} sid:{len(map_sid)}")
    return map_bdid, map_bid, map_cid, map_sid

# -------------------- TRANSFORM --------------------

def transform_doc(src: dict, maps) -> dict:
    map_bdid, map_bid, map_cid, map_sid = maps
    old_bdid = src.get("bdid")
    old_bid  = src.get("bid")
    old_cid  = src.get("cid")
    old_sid  = src.get("sid")

    out = dict(src)  # shallow copy
    if old_bdid is not None: out["o_bdid"] = old_bdid
    if old_bid  is not None: out["o_bid"]  = old_bid
    if old_cid  is not None: out["o_cid"]  = old_cid
    if old_sid  is not None: out["o_sid"]  = old_sid

    def as_int(v):
        try: return int(v)
        except Exception: return None

    if old_bdid is not None:
        key = as_int(old_bdid)
        out["bdid"] = map_bdid.get(key, str(old_bdid)) if key is not None else str(old_bdid)
    if old_bid is not None:
        key = as_int(old_bid)
        out["bid"]  = map_bid.get(key, str(old_bid)) if key is not None else str(old_bid)
    if old_cid is not None:
        key = as_int(old_cid)
        out["cid"]  = map_cid.get(key, str(old_cid)) if key is not None else str(old_cid)
    if old_sid is not None:
        key = as_int(old_sid)
        out["sid"]  = map_sid.get(key, str(old_sid)) if key is not None else str(old_sid)

    return out

# -------------------- PARALLEL REINDEX --------------------

def slice_actions_generator(es: Elasticsearch, source_index: str, dest_index: str,
                            maps, slice_id: int, slices: int,
                            query: Optional[dict], scroll_size: int) -> Iterable[dict]:
    q = query or {"query": {"match_all": {}}}
    q = {"slice": {"id": slice_id, "max": slices}, **q}
    for doc in scan(
        es,
        index=source_index,
        query=q,
        size=scroll_size,
        request_timeout=180,
        preserve_order=False,
    ):
        _id = doc.get("_id")
        src = doc.get("_source", {})
        out = transform_doc(src, maps)
        yield {"_op_type": "index", "_index": dest_index, "_id": _id, "_source": out}

def run_slice(es: Elasticsearch, source_index: str, dest_index: str,
              maps, slice_id: int, slices: int,
              query: Optional[dict], scroll_size: int,
              bulk_size: int, thread_count: int) -> Tuple[int, int]:
    """
    Returns: (success_count, error_count) for this slice.
    parallel_bulk sends multi-threaded bulks per slice.
    """
    # Set per-request options on the client (v8 way)
    es_bulk = es.options(request_timeout=600, retry_on_timeout=True)

    success = 0
    errors = 0
    for ok, item in parallel_bulk(
        es_bulk,
        slice_actions_generator(es, source_index, dest_index, maps, slice_id, slices, query, scroll_size),
        thread_count=thread_count,
        chunk_size=bulk_size,
        # no max_retries / retry_on_timeout here — those go on the client
        # keep kwargs minimal to avoid v8 signature issues
    ):
        if ok:
            success += 1
        else:
            errors += 1
    return success, errors


# -------------------- MAIN --------------------

def main():
    ap = argparse.ArgumentParser(description="FAST reindex + migrate using slicing & multi-threaded bulks.")
    ap.add_argument("-s", "--source", required=True, help="Source index")
    ap.add_argument("-d", "--dest", required=True, help="Destination index")
    ap.add_argument("--shards", type=int, help="Override number_of_shards on destination")
    ap.add_argument("--replicas", type=int, help="Override number_of_replicas on destination")
    ap.add_argument("--force", action="store_true", help="Reuse destination if it exists")

    ap.add_argument("--slices", type=int, default=4, help="Number of logical slices to scan in parallel")
    ap.add_argument("--slice-threads", type=int, default=1, help="Threads per slice for bulk sends")
    ap.add_argument("--scroll-size", type=int, default=2000, help="Scan page size per slice")
    ap.add_argument("--bulk-size", type=int, default=4000, help="Bulk chunk size per thread")

    ap.add_argument("--q", "--query", dest="query_json", help="Optional ES query JSON to filter docs")
    ap.add_argument("--optimize-dest", action="store_true", help="Temporarily set replicas=0, refresh_interval=-1")

    args = ap.parse_args()

    cfg = load_env()
    es = es_client(cfg["es"])

    # Ensure source
    if not es.indices.exists(index=args.source).body:
        print(f"Source index not found: {args.source}", file=sys.stderr)
        sys.exit(1)

    try:
        # Start time
        t0 = time.time()
        start_iso = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
        print(f"START: {start_iso}")

        # Build destination
        analysis, ct_mapping = fetch_source_analysis_and_ct(es, args.source)
        dest_mappings = build_destination_mapping(ct_mapping)
        dest_settings = build_destination_settings(analysis, args.shards, args.replicas)
        ensure_dest_index(es, args.dest, dest_mappings, dest_settings, force=args.force)

        if args.optimize_dest:
            optimize_dest_index(es, args.dest, enable=True)
            print("Destination optimized for ingest (replicas=0, refresh_interval=-1)")

        # Load PG mappings
        map_bdid, map_bid, map_cid, map_sid = load_pg_mappings(cfg["pg"])

        # Optional query
        query = None
        if args.query_json:
            try:
                query = json.loads(args.query_json)
            except json.JSONDecodeError as e:
                print(f"Invalid --q/--query JSON: {e}", file=sys.stderr)
                sys.exit(2)

        # Run slices in parallel
        print(f"Running with slices={args.slices}, slice_threads={args.slice_threads}, "
              f"scroll_size={args.scroll_size}, bulk_size={args.bulk_size}")

        totals_success = 0
        totals_errors = 0
        with ThreadPoolExecutor(max_workers=args.slices) as pool:
            futures = []
            for sid in range(args.slices):
                futures.append(
                    pool.submit(
                        run_slice, es, args.source, args.dest,
                        (map_bdid, map_bid, map_cid, map_sid),
                        sid, args.slices, query,
                        args.scroll_size, args.bulk_size, args.slice_threads
                    )
                )
            for fut in as_completed(futures):
                s, e = fut.result()
                totals_success += s
                totals_errors  += e
                print(f"Slice done: success={s}, errors={e}")

        # Finalize
        es.indices.refresh(index=args.dest)
        if args.optimize_dest:
            optimize_dest_index(es, args.dest, enable=False)
            print("Destination refresh_interval restored to 1s")

        t1 = time.time()
        end_iso = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
        dur = t1 - t0
        print(f"END:   {end_iso}")
        print(f"TOTAL successful bulks: {totals_success}  |  TOTAL errors: {totals_errors}")
        print(f"DURATION: {dur:.2f} seconds (~{dur/60:.2f} minutes)")

    except AuthenticationException as e:
        print(f"Authentication failed: {e}", file=sys.stderr); sys.exit(1)
    except (BadRequestError, NotFoundError) as e:
        print(f"Elasticsearch error: {e}", file=sys.stderr); sys.exit(1)
    except psycopg2.Error as e:
        print(f"Postgres error: {e}", file=sys.stderr); sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr); sys.exit(1)

if __name__ == "__main__":
    main()
