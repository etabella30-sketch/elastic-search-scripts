#!/usr/bin/env python3
import os
import sys
import logging
import concurrent.futures as futures
from typing import Iterable, Iterator, List, Optional, Tuple
from threading import Lock

from dotenv import load_dotenv
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError
import psycopg2
import psycopg2.extras


# ----------------------------
# Utilities
# ----------------------------

def chunked(seq: Iterable, size: int) -> Iterator[list]:
    buf = []
    for item in seq:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

def print_progress(prefix: str, done: int, total: int) -> None:
    # single-line progress like: "[fetch] 123/456"
    width = len(str(total))
    msg = f"[{prefix}] {str(done).rjust(width,' ')}/{total}\r"
    sys.stderr.write(msg)
    sys.stderr.flush()
    if done >= total:
        sys.stderr.write("\n")
        sys.stderr.flush()


# ----------------------------
# S3 (DigitalOcean Spaces)
# ----------------------------

def make_s3_client():
    aws_region = os.getenv("AWS_REGION")
    endpoint_url = os.getenv("AWS_ENDPOINT_URL")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not all([aws_region, endpoint_url, access_key, secret_key]):
        logging.error("Missing one or more AWS_* environment variables.")
        sys.exit(1)

    cfg = Config(
        region_name=aws_region,
        s3={"addressing_style": "virtual"},
        retries={"max_attempts": 5, "mode": "standard"},
        signature_version="s3v4",
        connect_timeout=10,
        read_timeout=30,
    )
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=cfg,
    )

def head_size(s3, bucket: str, key: str, max_retries: int = 3) -> Optional[int]:
    """Return object size in bytes (int) or None if not found / error."""
    attempt = 0
    while True:
        try:
            resp = s3.head_object(Bucket=bucket, Key=key)
            return int(resp["ContentLength"])
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                logging.warning(f"Not found: s3://{bucket}/{key}")
                return None
            attempt += 1
            if attempt > max_retries:
                logging.error(f"head_object failed after retries for {key}: {e}")
                return None
        except EndpointConnectionError as e:
            attempt += 1
            if attempt > max_retries:
                logging.error(f"Endpoint error after retries for {key}: {e}")
                return None


# ----------------------------
# Postgres
# ----------------------------

def make_pg_conn():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    dbname = os.getenv("POSTGRES_DB", "postgres")  # default if not set

    dsn = (
        f"host={host} port={port} dbname={dbname} user={user} password={password} "
        "sslmode=require"
    )
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = False
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Postgres: {e}")
        sys.exit(1)

FETCH_SQL = '''
select "cPath","nBundledetailid"
from "BundleDetail"
where nullif("cFilesize",'') is null
'''

UPDATE_SQL = '''
update "BundleDetail"
set "cFilesize" = %s
where "nBundledetailid" = %s
'''


# ----------------------------
# Worker
# ----------------------------

def worker_head_size(args) -> Tuple[str, str, Optional[int]]:
    """Return (key, pk, size)."""
    key, pk, bucket = args
    size = head_size(_S3_CLIENT, bucket, key)
    return key, pk, size


def main():
    load_dotenv(override=True)
    setup_logging()

    bucket = os.getenv("AWS_BUCKET")
    if not bucket:
        logging.error("AWS_BUCKET not set in environment.")
        sys.exit(1)

    # Init clients
    global _S3_CLIENT
    _S3_CLIENT = make_s3_client()
    pg = make_pg_conn()

    # Fetch rows to process
    with pg.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(FETCH_SQL)
        rows = cur.fetchall()
    total = len(rows)
    if total == 0:
        logging.info('Nothing to update. "cFilesize" is already set for all rows.')
        return

    logging.info(f"Found {total} rows to backfill.")

    # Concurrency & batching
    max_workers = int(os.getenv("MAX_WORKERS", max(8, (os.cpu_count() or 2) * 5)))
    batch_size = int(os.getenv("BATCH_SIZE", 200))

    # Jobs
    jobs = [(r["cPath"].lstrip("/"), str(r["nBundledetailid"]), bucket) for r in rows]

    # ----------------------------
    # Concurrent head requests w/ progress
    # ----------------------------
    results: List[Tuple[str, str, Optional[int]]] = []
    done = 0
    lock = Lock()

    with futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_to_job = {ex.submit(worker_head_size, j): j for j in jobs}
        for fut in futures.as_completed(future_to_job):
            res = fut.result()
            results.append(res)
            with lock:
                done += 1
                print_progress("fetch", done, total)

    # ----------------------------
    # Build updates (keep size as plain digits text)
    # ----------------------------
    updates: List[Tuple[str, str]] = []
    missing: List[Tuple[str, str]] = []
    for key, pk, size in results:
        if size is None:
            missing.append((key, pk))
        else:
            # IMPORTANT: store exactly as digits (e.g., "763246"), no formatting
            updates.append((str(size), pk))

    logging.info(f"Sizes resolved: {len(updates)}; missing/not found: {len(missing)}")

    # ----------------------------
    # Batched DB updates w/ progress
    # ----------------------------
    updated = 0
    total_updates = len(updates)
    print_progress("update", updated, total_updates)

    with pg.cursor() as cur:
        for batch in chunked(updates, batch_size):
            psycopg2.extras.execute_batch(cur, UPDATE_SQL, batch, page_size=len(batch))
            pg.commit()
            updated += len(batch)
            print_progress("update", updated, total_updates)

    if missing:
        pct = (len(missing) / total) * 100.0
        logging.warning(f"{len(missing)} objects missing or failed (~{pct:.1f}%).")

    pg.close()
    logging.info("Done.")


if __name__ == "__main__":
    main()
