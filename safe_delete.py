#!/usr/bin/env python3
import os
import sys
import json
import argparse
import warnings
from elasticsearch import Elasticsearch, ElasticsearchWarning
from elasticsearch.exceptions import AuthenticationException, NotFoundError, TransportError
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

def resolve_exact_field(es: Elasticsearch, index: str, base_field: str):
    """
    Prefer base_field+'.keyword' if it exists and is keyword; otherwise use base_field.
    Falls back to base_field as given if mapping can't be fetched.
    """
    try:
        m = es.indices.get_mapping(index=index)
        # ES 8 client may wrap in ObjectApiResponse; get dict body if present
        m = getattr(m, "body", m)
        # find first index mapping (works even if index is an alias to 1 target)
        idx_name, idx_map = next(iter(m.items()))
        props = idx_map.get("mappings", {}).get("properties", {})
        if base_field in props:
            # if subfield "keyword" exists and type keyword, use it
            sub = props[base_field].get("fields", {})
            if "keyword" in sub and sub["keyword"].get("type") == "keyword":
                return f"{base_field}.keyword"
            # if base field itself is keyword
            if props[base_field].get("type") == "keyword":
                return base_field
        # if base field not present, still return base_field to let query run (may match 0)
        return base_field
    except Exception:
        # On mapping errors, don't block; return base_field
        return base_field

def build_query(field: str, value: str, case_insensitive=False):
    """
    Use term for exact match on keyword field. If case_insensitive requested,
    use 'terms' with normalizer is not guaranteed; we fallback to a simple 'term'
    with 'case_insensitive' supported on text/keyword in newer ES versions.
    """
    term_body = {"value": value}
    if case_insensitive:
        term_body["case_insensitive"] = True
    return {"query": {"term": {field: term_body}}}

def count_matches(es: Elasticsearch, index: str, query: dict) -> int:
    resp = es.count(index=index, body=query)
    return resp.get("count", 0)

def sample_ids(es: Elasticsearch, index: str, query: dict, size: int = 10):
    # Pull a few _ids (no _source) for preview
    resp = es.search(
        index=index,
        body={**query, "_source": False, "size": size, "sort": ["_doc"]},
    )
    hits = resp.get("hits", {}).get("hits", [])
    return [h.get("_id") for h in hits]

def delete_by_query_safe(
    es: Elasticsearch,
    index: str,
    query: dict,
    refresh=True,
    conflicts="proceed",
    batch_size=1000,
    max_docs=None
):
    # If max_docs set, use 'slice' + count check, but ES delete_by_query doesn't support direct max_docs cap.
    # We'll enforce by aborting if count > max_docs before executing.
    params = {"refresh": refresh, "conflicts": conflicts, "slices": "auto", "timeout": "2m", "wait_for_completion": True, "requests_per_second": -1, "batch_size": batch_size}
    return es.delete_by_query(index=index, body=query, **params)

def main():
    parser = argparse.ArgumentParser(description="Safely delete documents by exact bdid match with confirmation.")
    parser.add_argument("-i", "--index", required=True, help="Exact index name (no wildcards unless --allow-pattern).")
    parser.add_argument("-b", "--bdid", required=True, help='bdid value to delete (e.g. "239356").')
    parser.add_argument("--field", default="bdid", help='Base field name (default: "bdid").')
    parser.add_argument("--case-insensitive", action="store_true", help="Case-insensitive term match (if supported).")
    parser.add_argument("--max-delete", type=int, default=50000, help="Abort if matches exceed this number (default: 50,000).")
    parser.add_argument("--allow-pattern", action="store_true", help="Allow wildcard/alias that expands to multiple indices.")
    parser.add_argument("--dry-run", action="store_true", help="Show count and sample _ids; do not delete.")
    args = parser.parse_args()

    if any(ch in args.index for ch in ["*", "?", ","]) and not args.allow_pattern:
        print("Refusing to run on patterns or multiple indices without --allow-pattern.")
        sys.exit(3)

    es = make_client()

    try:
        # Resolve exact field (prefer keyword subfield)
        exact_field = resolve_exact_field(es, args.index, args.field)

        query = build_query(exact_field, args.bdid, case_insensitive=args.case_insensitive)

        # Count matches first
        cnt = count_matches(es, args.index, query)
        print(f"Target index: {args.index}")
        print(f"Field used  : {exact_field}")
        print(f"bdid value  : {args.bdid}")
        print(f"Match count : {cnt}")

        # Basic safety checks
        if cnt == 0:
            print("No matching documents found. Nothing to delete.")
            sys.exit(0)

        if cnt > args.max_delete:
            print(f"ABORT: Match count {cnt:,} exceeds --max-delete={args.max_delete:,}.")
            print("If this is expected, re-run with a larger --max-delete or refine your query.")
            sys.exit(4)

        # Show a few sample IDs
        ids = sample_ids(es, args.index, query, size=min(10, cnt))
        print("\nSample matching _ids:")
        for _id in ids:
            print(f"  {_id}")

        if args.dry_run:
            print("\nDry-run mode enabled: no deletions performed.")
            sys.exit(0)

        # Interactive confirmation
        print("\nType the bdid value to confirm deletion (or press Enter to cancel):")
        typed = input("> ").strip()
        if typed != args.bdid:
            print("Confirmation failed or canceled. No documents were deleted.")
            sys.exit(0)

        # Perform deletion
        print("Deleting... (this may take a moment)")
        result = delete_by_query_safe(es, args.index, query)
        # Pretty print a minimal summary
        summary = {
            "took_ms": result.get("took"),
            "deleted": result.get("deleted"),
            "version_conflicts": result.get("version_conflicts"),
            "batches": result.get("batches"),
            "total": result.get("total"),
            "timed_out": result.get("timed_out"),
            "failures": result.get("failures"),
        }
        print("\nDelete by query result (summary):")
        print(json.dumps(summary, indent=2))

        # Optional: force a refresh already done via refresh=True

    except AuthenticationException as e:
        print(f"Authentication failed: {e}")
        sys.exit(1)
    except NotFoundError:
        print(f"Index not found: {args.index}")
        sys.exit(1)
    except TransportError as e:
        print(f"Transport error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nCanceled by user. No further action taken.")
        sys.exit(130)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
