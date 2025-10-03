import os
import sys
import json
import argparse
import warnings
from typing import Dict, Tuple, Optional, Iterable

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, ElasticsearchWarning
from elasticsearch.exceptions import AuthenticationException, NotFoundError, BadRequestError
from elasticsearch.helpers import scan, bulk

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
        "pg": {
            "host": pg_host, "port": pg_port, "user": pg_user,
            "password": pg_pass, "database": pg_db,
            "sql_file": sql_file, "sql_text": sql_text
        },
    }


def es_client(es_conf) -> Elasticsearch:
    warnings.simplefilter("ignore", ElasticsearchWarning)
    return Elasticsearch(
        hosts=[es_conf["hosts"]],
        basic_auth=(es_conf["user"], es_conf["password"]),
        verify_certs=False
    )


# -------------------- ES MAPPING/SETTINGS BUILDERS --------------------

def fetch_source_analysis_and_ct(es: Elasticsearch, source_index: str):
    """Get 'ct' mapping and analysis settings from the source index."""
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
    """Target mapping: o_* integers; new ids as keyword; keep ct subfields; keep pg integer."""
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


# -------------------- POSTGRES LOADER --------------------

def read_sql(pg_conf: dict) -> str:
    if pg_conf["sql_file"]:
        with open(pg_conf["sql_file"], "r", encoding="utf-8") as f:
            return f.read()
    return pg_conf["sql_text"]


def load_pg_mappings(pg_conf: dict) -> Tuple[Dict[int, str], Dict[int, str], Dict[int, str], Dict[int, str]]:
    """
    Load mapping rows from Postgres using aliases:
      bdid (text), bid (text), cid (text), sid (text),
      o_bdid (int), o_bid (int), o_cid (int), o_sid (int)
    Returns dicts: map_bdid, map_bid, map_cid, map_sid : old-int -> new-string
    """
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
    for r in rows:
        # New values as strings (UUID or any text)
        new_bdid = r.get("bdid")
        new_bid  = r.get("bid")
        new_cid  = r.get("cid")
        new_sid  = r.get("sid")

        # Old values expected to be integers; safely coerce
        def to_int(v):
            try:
                return int(v) if v is not None and str(v).strip() != "" else None
            except Exception:
                return None

        old_bdid = to_int(r.get("o_bdid"))
        old_bid  = to_int(r.get("o_bid"))
        old_cid  = to_int(r.get("o_cid"))
        old_sid  = to_int(r.get("o_sid"))

        if old_bdid is not None and new_bdid is not None:
            map_bdid[old_bdid] = str(new_bdid)
        if old_bid is not None and new_bid is not None:
            map_bid[old_bid] = str(new_bid)
        if old_cid is not None and new_cid is not None:
            map_cid[old_cid] = str(new_cid)
        if old_sid is not None and new_sid is not None:
            map_sid[old_sid] = str(new_sid)

    cur.close()
    conn.close()

    print(f"Loaded {len(rows)} mapping rows from Postgres.")
    print(f"Distinct mappings — bdid:{len(map_bdid)} bid:{len(map_bid)} cid:{len(map_cid)} sid:{len(map_sid)}")
    return map_bdid, map_bid, map_cid, map_sid


# -------------------- TRANSFORM & BULK --------------------

def transform_doc(src: dict, maps) -> dict:
    """Transform a single _source doc."""
    map_bdid, map_bid, map_cid, map_sid = maps

    old_bdid = src.get("bdid")
    old_bid  = src.get("bid")
    old_cid  = src.get("cid")
    old_sid  = src.get("sid")

    out = dict(src)  # shallow copy

    # copy original ints into o_*
    if old_bdid is not None:
        out["o_bdid"] = old_bdid
    if old_bid is not None:
        out["o_bid"] = old_bid
    if old_cid is not None:
        out["o_cid"] = old_cid
    if old_sid is not None:
        out["o_sid"] = old_sid

    # new keyword ids via mapping (fallback to stringified original)
    def as_int(v):
        try:
            return int(v)
        except Exception:
            return None

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

    # ct and pg remain unchanged
    return out


def actions_stream(es: Elasticsearch, source_index: str, dest_index: str, maps, query: Optional[dict], scroll_size: int) -> Iterable[dict]:
    for doc in scan(
        es,
        index=source_index,
        query=query or {"query": {"match_all": {}}},
        size=scroll_size,
        preserve_order=False,
        request_timeout=120,
    ):
        _id = doc.get("_id")
        src = doc.get("_source", {})
        out = transform_doc(src, maps)
        yield {
            "_op_type": "index",
            "_index": dest_index,
            "_id": _id,           # preserve original _id
            "_source": out
        }


# -------------------- MAIN --------------------

def main():
    ap = argparse.ArgumentParser(description="Reindex + migrate fields/types using Postgres ID mapping.")
    ap.add_argument("-s", "--source", required=True, help="Source index (e.g., case_documents_1050)")
    ap.add_argument("-d", "--dest", required=True, help="Destination index (new mapping)")
    ap.add_argument("--shards", type=int, help="Override number_of_shards")
    ap.add_argument("--replicas", type=int, help="Override number_of_replicas")
    ap.add_argument("--force", action="store_true", help="Reuse destination if it exists")
    ap.add_argument("--scroll-size", type=int, default=1000, help="Scan page size")
    ap.add_argument("--bulk-size", type=int, default=2000, help="Bulk chunk size")
    ap.add_argument("--q", "--query", dest="query_json", help="Optional ES query JSON to filter docs")
    args = ap.parse_args()

    cfg = load_env()
    es = es_client(cfg["es"])

    # Check source index exists
    if not es.indices.exists(index=args.source).body:
        print(f"Source index not found: {args.source}", file=sys.stderr)
        sys.exit(1)

    try:
        # 1) build destination mapping/settings from source 'ct' + analysis
        analysis, ct_mapping = fetch_source_analysis_and_ct(es, args.source)
        dest_mappings = build_destination_mapping(ct_mapping)
        dest_settings = build_destination_settings(analysis, args.shards, args.replicas)

        # 2) create/reuse destination
        ensure_dest_index(es, args.dest, dest_mappings, dest_settings, force=args.force)

        # 3) load mapping rows from Postgres
        map_bdid, map_bid, map_cid, map_sid = load_pg_mappings(cfg["pg"])

        # 4) optional ES filter query
        query = None
        if args.query_json:
            try:
                query = json.loads(args.query_json)
            except json.JSONDecodeError as e:
                print(f"Invalid --q/--query JSON: {e}", file=sys.stderr)
                sys.exit(2)

        # 5) stream + bulk
        print("Starting migration reindex...")
        success, errors = bulk(
            es,
            actions_stream(es, args.source, args.dest, (map_bdid, map_bid, map_cid, map_sid), query, args.scroll_size),
            chunk_size=args.bulk_size,
            request_timeout=600,
            refresh=False
        )
        print(f"Bulk completed. Successful actions: {success}")
        if errors:
            print("Some bulk errors occurred (see logs above).")

        es.indices.refresh(index=args.dest)
        print(f"Done. Destination '{args.dest}' refreshed.")

    except AuthenticationException as e:
        print(f"Authentication failed: {e}", file=sys.stderr)
        sys.exit(1)
    except (BadRequestError, NotFoundError) as e:
        print(f"Elasticsearch error: {e}", file=sys.stderr)
        sys.exit(1)
    except psycopg2.Error as e:
        print(f"Postgres error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
