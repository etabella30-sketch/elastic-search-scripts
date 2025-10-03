#!/usr/bin/env python3
import os
import sys
import json
import time
import argparse
import warnings
import logging
from datetime import datetime, timezone
from typing import Dict, Tuple, Optional, Iterable, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, ElasticsearchWarning
from elasticsearch.exceptions import AuthenticationException, NotFoundError, BadRequestError
from elasticsearch.helpers import scan, parallel_bulk

import psycopg2
import psycopg2.extras

# -------------------- LOGGING --------------------

def setup_logger(log_file: Optional[str]) -> logging.Logger:
    logger = logging.getLogger("migrate_multi")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    # Console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File
    if log_file:
        # Create folder if needed
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.isdir(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger

# -------------------- ENV & CLIENTS --------------------

def load_env(logger: logging.Logger):
    load_dotenv()
    es_hosts = os.getenv("ELASTICSEARCH_HOSTS")
    es_user  = os.getenv("ELASTICSEARCH_USER")
    es_pass  = os.getenv("ELASTICSEARCH_PASSWORD")
    if not all([es_hosts, es_user, es_pass]):
        logger.error("Missing ES env vars: ELASTICSEARCH_HOSTS, ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD")
        sys.exit(2)

    pg_host = os.getenv("POSTGRES_HOST")
    pg_port = int(os.getenv("POSTGRES_PORT", "5432"))
    pg_user = os.getenv("POSTGRES_USER")
    pg_pass = os.getenv("POSTGRES_PASSWORD")
    pg_db   = os.getenv("POSTGRES_DB")
    sql_file = os.getenv("POSTGRES_SQL_FILE")
    sql_text = os.getenv("POSTGRES_SQL")

    if not all([pg_host, pg_user, pg_pass, pg_db]):
        logger.error("Missing Postgres env vars: POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB")
        sys.exit(2)

    return {
        "es": {"hosts": es_hosts, "user": es_user, "password": es_pass},
        "pg": {"host": pg_host, "port": pg_port, "user": pg_user, "password": pg_pass,
               "database": pg_db, "sql_file": sql_file, "sql_text": sql_text}
    }

def es_client(es_conf) -> Elasticsearch:
    warnings.simplefilter("ignore", ElasticsearchWarning)
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
    props = mappings.get("properties", {}) or {}
    ct_mapping = props.get("ct")
    if not ct_mapping:
        raise RuntimeError(f"Index '{source_index}' mapping must contain a 'ct' field.")
    s = es.indices.get_settings(index=source_index, flat_settings=False, include_defaults=False)
    settings_full = getattr(s, "body", s)[source_index].get("settings", {})
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
    settings: Dict[str, dict] = {}
    if analysis:
        settings["analysis"] = analysis
    settings.setdefault("index", {})
    if shards is not None:
        settings["index"]["number_of_shards"] = shards
    if replicas is not None:
        settings["index"]["number_of_replicas"] = replicas
    return settings

def ensure_dest_index(es: Elasticsearch, dest_index: str, mappings: dict, settings: dict, force: bool, logger: logging.Logger) -> bool:
    """
    Create destination index or reuse it if --force.
    Returns True if index is ready to use; False if it exists and force=False (caller should skip).
    """
    exists = es.indices.exists(index=dest_index).body
    if exists and not force:
        logger.warning(f"Destination '{dest_index}' exists. Skipping (use --force to reuse).")
        return False
    if not exists:
        es.indices.create(index=dest_index, mappings=mappings, settings=settings)
        logger.info(f"Created destination index '{dest_index}'")
    else:
        logger.info(f"Reusing existing destination index '{dest_index}' (--force)")
    return True

def optimize_dest_index(es: Elasticsearch, dest_index: str, enable: bool, logger: logging.Logger):
    """Temporarily set replicas=0 and refresh_interval=-1 for faster writes, then restore."""
    if enable:
        es.indices.put_settings(
            index=dest_index,
            settings={"index": {"number_of_replicas": 0, "refresh_interval": "-1"}}
        )
        logger.info("  -> Destination optimized for ingest (replicas=0, refresh_interval=-1)")
    else:
        es.indices.put_settings(
            index=dest_index,
            settings={"index": {"refresh_interval": "1s"}}
        )
        logger.info("  -> refresh_interval restored to 1s")

# -------------------- POSTGRES LOADER --------------------

def read_sql(pg_conf: dict) -> str:
    if pg_conf.get("sql_file"):
        with open(pg_conf["sql_file"], "r", encoding="utf-8") as f:
            return f.read()
    if pg_conf.get("sql_text"):
        return pg_conf["sql_text"]
    raise RuntimeError("No SQL provided (need sql_file or sql_text)")

def load_pg_mappings(pg_conf: dict, logger: logging.Logger) -> Tuple[Dict[int, str], Dict[int, str], Dict[int, str], Dict[int, str]]:
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
    logger.info(f"Loaded {len(rows)} mapping rows from Postgres. "
                f"Distinct mappings — bdid:{len(map_bdid)} bid:{len(map_bid)} cid:{len(map_cid)} sid:{len(map_sid)}")
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
              bulk_size: int, thread_count: int, logger: logging.Logger) -> Tuple[int, int]:
    """
    Returns: (success_bulk_count, error_bulk_count) for this slice.
    """
    es_bulk = es.options(request_timeout=600, retry_on_timeout=True)

    success = 0
    errors = 0
    for ok, item in parallel_bulk(
        es_bulk,
        slice_actions_generator(es, source_index, dest_index, maps, slice_id, slices, query, scroll_size),
        thread_count=thread_count,
        chunk_size=bulk_size,
    ):
        if ok:
            success += 1
        else:
            errors += 1
            # Compact error logging
            try:
                action, result = next(iter(item.items()))
                reason = result.get('error', {}).get('reason', str(result.get('error')))
                logger.error(f"[bulk-error] action={action} index={result.get('_index')} id={result.get('_id')} reason={reason}")
            except Exception:
                logger.error(f"[bulk-error] item={item}")
    return success, errors

# -------------------- UTILITIES FOR MULTI-INDEX --------------------

def parse_sources_arg(s: Optional[str]) -> List[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]

def read_sources_file(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise RuntimeError("--sources-file must contain a JSON array")
    return [str(x) for x in data]

def extract_zncaseid_from_index(index_name: str) -> int:
    # expects pattern like case_documents_1163
    try:
        return int(index_name.rsplit("_", 1)[-1])
    except Exception:
        raise RuntimeError(f"Cannot parse ZnCaseid from index name: {index_name}")

def render_sql_template(sql_template_text: str, zncaseid: int) -> str:
    return sql_template_text.format(zncaseid=zncaseid)

def get_unique_cid_from_maps(map_cid: Dict[int, str]) -> str:
    # map_cid maps old_cid(int)->new_cid(str). We need the new cid values.
    vals = sorted(set(map_cid.values()))
    if len(vals) == 0:
        raise RuntimeError("Mapping query returned no rows (no cid found).")
    if len(vals) > 1:
        raise RuntimeError(f"Mapping produced multiple distinct cid values: {vals}")
    return vals[0]

def count_docs(es: Elasticsearch, index: str, query: Optional[dict]) -> int:
    body = query or {"query": {"match_all": {}}}
    # ES 8: count expects {"query": {...}}
    return es.count(index=index, body=body)["count"]

# -------------------- MAIN (MULTI-INDEX DRIVER) --------------------

def main():
    ap = argparse.ArgumentParser(description="Batch reindex multiple indices using a SQL template and dynamic dest names.")
    # Sources
    ap.add_argument("--sources", help="Comma-separated list of source indices")
    ap.add_argument("--sources-file", help="Path to JSON array of source indices")

    # Destination and alias options
    ap.add_argument("--dest-template", default="case_documents_new_{cid}",
                    help="Destination index name pattern. Vars: {cid}, {zncaseid}, {source}")
    ap.add_argument("--alias-template",
                    help="Optional alias to point at dest after reindex. Vars: {cid}, {zncaseid}, {source}")

    # Reindex performance knobs
    ap.add_argument("--shards", type=int)
    ap.add_argument("--replicas", type=int)
    ap.add_argument("--force", action="store_true", help="Reuse destination if it exists")
    ap.add_argument("--slices", type=int, default=4, help="Scan slices per index")
    ap.add_argument("--slice-threads", type=int, default=1, help="Bulk sender threads per slice")
    ap.add_argument("--scroll-size", type=int, default=2000, help="Scan page size per slice")
    ap.add_argument("--bulk-size", type=int, default=4000, help="Bulk chunk size per thread")
    ap.add_argument("--q", "--query", dest="query_json", help="Optional ES query JSON to filter docs")
    ap.add_argument("--optimize-dest", action="store_true", help="Temporarily set replicas=0, refresh_interval=-1")
    ap.add_argument("--dry-run", action="store_true", help="Plan only, no writes")

    # SQL templating
    ap.add_argument("--sql-template-file", help="Path to SQL template containing {zncaseid}")
    ap.add_argument("--sql-template", help="Inline SQL template containing {zncaseid}")

    # Logging
    ap.add_argument("--log-file", default="migrate_multi.log", help="Path to log file")

    args = ap.parse_args()
    logger = setup_logger(args.log_file)

    # Collect sources
    sources: List[str] = []
    sources += parse_sources_arg(args.sources)
    if args.sources_file:
        sources += read_sources_file(args.sources_file)
    if not sources:
        logger.error("Provide at least one source index via --sources or --sources-file")
        sys.exit(2)

    # Load env & clients
    cfg = load_env(logger)
    es = es_client(cfg["es"])

    # Load SQL template
    if args.sql_template_file:
        with open(args.sql_template_file, "r", encoding="utf-8") as f:
            sql_template_text = f.read()
    elif args.sql_template:
        sql_template_text = args.sql_template
    else:
        logger.error("Provide --sql-template-file or --sql-template")
        sys.exit(2)

    # Optional query (same for all)
    query = None
    if args.query_json:
        try:
            query = json.loads(args.query_json)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid --q/--query JSON: {e}")
            sys.exit(2)

    # Batch stats
    batch_start = time.time()
    total_indices = len(sources)
    processed_ok = 0
    processed_skip = 0
    processed_err = 0

    logger.info(f"=== BATCH START === total_indices={total_indices}")

    for idx, source_index in enumerate(sources, start=1):
        index_start = time.time()
        logger.info("=" * 80)
        logger.info(f"[{idx}/{total_indices}] START  source={source_index}")

        try:
            if not es.indices.exists(index=source_index).body:
                logger.warning(f"[{idx}/{total_indices}] SKIP   source not found")
                processed_skip += 1
                continue

            zncaseid = extract_zncaseid_from_index(source_index)
            sql_text = render_sql_template(sql_template_text, zncaseid)

            # Build PG config override with the rendered SQL
            pg_conf = dict(cfg["pg"])
            pg_conf["sql_file"] = None
            pg_conf["sql_text"] = sql_text

            # Prepare destination mapping/settings borrowed from source
            analysis, ct_mapping = fetch_source_analysis_and_ct(es, source_index)
            dest_mappings = build_destination_mapping(ct_mapping)
            dest_settings = build_destination_settings(analysis, args.shards, args.replicas)

            # Count docs in source (respecting optional query)
            try:
                src_docs = count_docs(es, source_index, query)
            except Exception as e:
                logger.warning(f"[{idx}/{total_indices}] Could not count source docs, proceeding: {e}")
                src_docs = -1

            # Load PG mappings for *this* case (must resolve to exactly one cid)
            map_bdid, map_bid, map_cid, map_sid = load_pg_mappings(pg_conf, logger)
            cid = get_unique_cid_from_maps(map_cid)  # ensure single cid
            dest_index = args.dest_template.format(cid=cid, zncaseid=zncaseid, source=source_index)

            if dest_index == source_index:
                raise RuntimeError(
                    f"Destination index equals source index ({source_index}). "
                    f"Change --dest-template (e.g., 'case_documents_new_{cid}') or use aliases."
                )

            logger.info(f"[{idx}/{total_indices}] PLAN   zncaseid={zncaseid} cid={cid} dest={dest_index} src_docs={src_docs}")

            if args.dry_run:
                logger.info(f"[{idx}/{total_indices}] DRY RUN: skipping create/reindex/alias")
                processed_ok += 1
                continue

            # Create or reuse destination
            ready = ensure_dest_index(es, dest_index, dest_mappings, dest_settings, force=args.force, logger=logger)
            if not ready:
                processed_skip += 1
                continue

            if args.optimize_dest:
                optimize_dest_index(es, dest_index, enable=True, logger=logger)

            # Reindex using slices + parallel bulk
            logger.info(f"[{idx}/{total_indices}] REINDEX slices={args.slices}, slice_threads={args.slice_threads}, "
                        f"scroll_size={args.scroll_size}, bulk_size={args.bulk_size}")

            totals_success = 0
            totals_errors = 0
            with ThreadPoolExecutor(max_workers=args.slices) as pool:
                futures = []
                for sid in range(args.slices):
                    futures.append(
                        pool.submit(
                            run_slice, es, source_index, dest_index,
                            (map_bdid, map_bid, map_cid, map_sid),
                            sid, args.slices, query,
                            args.scroll_size, args.bulk_size, args.slice_threads, logger
                        )
                    )
                for fut in as_completed(futures):
                    s, e = fut.result()
                    totals_success += s
                    totals_errors  += e
                    logger.info(f"[{idx}/{total_indices}] slice done: success_bulks={s} error_bulks={e}")

            es.indices.refresh(index=dest_index)
            if args.optimize_dest:
                optimize_dest_index(es, dest_index, enable=False, logger=logger)

            # Count docs in destination for completion ratio
            try:
                dest_docs = count_docs(es, dest_index, None)  # full count in dest
            except Exception as e:
                logger.warning(f"[{idx}/{total_indices}] Could not count dest docs: {e}")
                dest_docs = -1

            # Optional alias cutover
            if args.alias_template:
                alias = args.alias_template.format(cid=cid, zncaseid=zncaseid, source=source_index)
                try:
                    actions = [
                        {"remove": {"index": "*", "alias": alias}},
                        {"add":    {"index": dest_index, "alias": alias}},
                    ]
                    es.indices.update_aliases(body={"actions": actions})
                    logger.info(f"[{idx}/{total_indices}] Alias '{alias}' now points to '{dest_index}'")
                except Exception as e:
                    logger.error(f"[{idx}/{total_indices}] Alias update failed for '{alias}': {e}")

            # Finish per-index logging
            dur = time.time() - index_start
            if src_docs >= 0 and dest_docs >= 0 and src_docs > 0:
                pct = 100.0 * dest_docs / src_docs
                logger.info(f"[{idx}/{total_indices}] END    took={dur:.2f}s | docs {dest_docs}/{src_docs} ({pct:.2f}%) "
                            f"| bulks_ok={totals_success} bulks_err={totals_errors}")
            else:
                logger.info(f"[{idx}/{total_indices}] END    took={dur:.2f}s | bulks_ok={totals_success} bulks_err={totals_errors}")

            processed_ok += 1

        except AuthenticationException as e:
            logger.error(f"[{idx}/{total_indices}] AUTH ERROR: {e}")
            processed_err += 1
            continue
        except (BadRequestError, NotFoundError) as e:
            logger.error(f"[{idx}/{total_indices}] ES ERROR: {e}")
            processed_err += 1
            continue
        except psycopg2.Error as e:
            logger.error(f"[{idx}/{total_indices}] PG ERROR: {e}")
            processed_err += 1
            continue
        except Exception as e:
            logger.error(f"[{idx}/{total_indices}] ERROR: {e}")
            processed_err += 1
            continue

    batch_dur = time.time() - batch_start
    logger.info("=" * 80)
    logger.info(f"=== BATCH END === took={batch_dur:.2f}s | ok={processed_ok} skipped={processed_skip} errors={processed_err} of {total_indices}")

if __name__ == "__main__":
    main()
