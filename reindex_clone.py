import os
import sys
import json
import time
import argparse
import warnings
from elasticsearch import Elasticsearch, ElasticsearchWarning
from elasticsearch.exceptions import NotFoundError, BadRequestError, AuthenticationException
from dotenv import load_dotenv

def make_client():
    load_dotenv()
    hosts = os.getenv("ELASTICSEARCH_HOSTS")
    user  = os.getenv("ELASTICSEARCH_USER")
    pwd   = os.getenv("ELASTICSEARCH_PASSWORD")
    if not hosts or not user or not pwd:
        print("Missing env vars: ELASTICSEARCH_HOSTS, ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD")
        sys.exit(2)
    warnings.simplefilter("ignore", ElasticsearchWarning)
    return Elasticsearch(hosts=[hosts], basic_auth=(user, pwd), verify_certs=False)

def fetch_source_blueprint(es, source_index):
    # mappings
    m = es.indices.get_mapping(index=source_index)
    mappings = getattr(m, "body", m)[source_index]["mappings"]

    # settings (strip non-cloneable keys)
    s = es.indices.get_settings(index=source_index, flat_settings=False, include_defaults=False)
    settings_full = getattr(s, "body", s)[source_index]["settings"]

    # keep only index.* under "settings" and remove dynamic/uncloneable fields
    idx_settings = settings_full.get("index", {})
    for k in [
        "uuid", "version", "creation_date", "provided_name", "routing", "resize", "lifecycle",
        "legacy", "verbs", "verified_before_close"
    ]:
        idx_settings.pop(k, None)

    # Optional: you can override shard/replica counts later via args
    return mappings, {"index": idx_settings}

def create_dest_index(es, dest_index, mappings, settings, shards=None, replicas=None, force=False):
    # allow overrides
    if shards is not None:
        settings.setdefault("index", {})["number_of_shards"] = str(shards)
    if replicas is not None:
        settings.setdefault("index", {})["number_of_replicas"] = str(replicas)

    exists = es.indices.exists(index=dest_index).body
    if exists:
        if not force:
            print(f"Destination index '{dest_index}' already exists. Use --force to reuse it.")
            sys.exit(1)
        else:
            print(f"Destination index '{dest_index}' already exists; reusing (force).")
            return

    es.indices.create(index=dest_index, mappings=mappings, settings=settings)
    print(f"Created index '{dest_index}' with mappings & settings.")

def reindex_all(es, source_index, dest_index, slices="auto", refresh=True, wait=True):
    body = {
        "source": {"index": source_index},
        "dest": {"index": dest_index}
    }
    resp = es.reindex(
        body=body,
        slices=slices,
        refresh=refresh,
        wait_for_completion=wait
    )
    if wait:
        # v8 returns ObjectApiResponse with stats in body
        payload = getattr(resp, "body", resp)
        print("Reindex completed:")
        print(json.dumps(payload, indent=2))
    else:
        task = getattr(resp, "body", resp).get("task")
        print(f"Reindex started as task: {task}")
        return task

def main():
    p = argparse.ArgumentParser(description="Create a new index and copy (reindex) all data from a source index.")
    p.add_argument("-s", "--source", required=True, help="Source index name (exact)")
    p.add_argument("-d", "--dest", required=True, help="Destination index name (must not exist unless --force)")
    p.add_argument("--shards", type=int, help="Override number_of_shards for destination")
    p.add_argument("--replicas", type=int, help="Override number_of_replicas for destination")
    p.add_argument("--no-wait", action="store_true", help="Do not wait for reindex completion (returns task id)")
    p.add_argument("--no-refresh", action="store_true", help="Do not refresh destination index after reindex")
    p.add_argument("--force", action="store_true", help="Proceed if destination already exists (won't recreate)")
    args = p.parse_args()

    es = make_client()

    try:
        # sanity: ensure source exists
        if not es.indices.exists(index=args.source).body:
            print(f"Source index not found: {args.source}")
            sys.exit(1)

        mappings, settings = fetch_source_blueprint(es, args.source)
        create_dest_index(
            es,
            dest_index=args.dest,
            mappings=mappings,
            settings=settings,
            shards=args.shards,
            replicas=args.replicas,
            force=args.force
        )

        task = reindex_all(
            es,
            source_index=args.source,
            dest_index=args.dest,
            slices="auto",
            refresh=not args.no_refresh,
            wait=not args.no_wait
        )
        if task:
            print(f"Use: GET _tasks/{task} to check progress")

    except AuthenticationException as e:
        print(f"Authentication failed: {e}")
        sys.exit(1)
    except BadRequestError as e:
        print(f"Bad request: {e}")
        sys.exit(1)
    except NotFoundError as e:
        print(f"Not found: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
