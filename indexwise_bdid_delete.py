#!/usr/bin/env python3
import os
import sys
import argparse
import warnings
import json
from typing import List, Dict, Any, Tuple

from elasticsearch import Elasticsearch, ElasticsearchWarning
from elasticsearch.exceptions import AuthenticationException, NotFoundError, TransportError
from dotenv import load_dotenv

# ---------- Connection ----------
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

# ---------- Helpers ----------
def list_indices(es: Elasticsearch, pattern: str) -> List[str]:
    try:
        resp = es.cat.indices(index=pattern, format="json")
        return [r["index"] for r in resp]
    except NotFoundError:
        return []
    except Exception as e:
        print(f"Error listing indices for pattern '{pattern}': {e}")
        return []

def resolve_keyword_field(es: Elasticsearch, index: str, base_field: str) -> str:
    m = es.indices.get_mapping(index=index)
    m = getattr(m, "body", m)
    _, idx_map = next(iter(m.items()))
    props = idx_map.get("mappings", {}).get("properties", {})
    if base_field in props:
        fld = props[base_field]
        sub = fld.get("fields", {})
        if "keyword" in sub and sub["keyword"].get("type") == "keyword":
            return f"{base_field}.keyword"
        if fld.get("type") == "keyword":
            return base_field
        raise RuntimeError(
            f"Field '{base_field}' in index '{index}' is not keyword and has no 'keyword' subfield."
        )
    raise RuntimeError(f"Field '{base_field}' not found in mapping for index '{index}'.")

def build_regex_query(keyword_field: str, max_len: int) -> Dict[str, Any]:
    n = max(0, max_len - 1)
    return {
        "bool": {
            "must": [
                {"exists": {"field": keyword_field}},
                {"regexp": {keyword_field: f".{{0,{n}}}"}}
            ]
        }
    }

def count_matches(es: Elasticsearch, index: str, query: Dict[str, Any]) -> int:
    resp = es.count(index=index, body={"query": query})
    return resp.get("count", 0)

def composite_unique_values(
    es: Elasticsearch, index: str, keyword_field: str, query: Dict[str, Any], page_size: int = 1000
) -> List[str]:
    uniques: List[str] = []
    after_key = None
    while True:
        body = {
            "size": 0,
            "query": query,
            "aggs": {
                "uniq": {
                    "composite": {
                        "size": page_size,
                        "sources": [{"bdid": {"terms": {"field": keyword_field}}}]
                    }
                }
            }
        }
        if after_key:
            body["aggs"]["uniq"]["composite"]["after"] = after_key

        resp = es.search(index=index, body=body)
        agg = resp.get("aggregations", {}).get("uniq", {})
        buckets = agg.get("buckets", [])
        for b in buckets:
            key = b.get("key", {}).get("bdid")
            if key:
                uniques.append(key)
        after_key = agg.get("after_key")
        if not after_key or not buckets:
            break
    return uniques

def delete_by_query(es: Elasticsearch, index: str, query: Dict[str, Any]):
    body = {"query": query}
    return es.delete_by_query(
        index=index,
        body=body,
        refresh=True,
        conflicts="proceed",
        slices="auto",
        wait_for_completion=True,
        timeout="5m",
        requests_per_second=-1,
        batch_size=1000,
    )

# ---------- Main ----------
def main():
    parser = argparse.ArgumentParser(
        description="List unique short bdids (index,bdid) and optionally DELETE docs where bdid length < N across indices."
    )
    parser.add_argument("--pattern", default="caseindex_*", help="Index name pattern (default: caseindex_*)")
    parser.add_argument("--field", default="bdid", help="Base field name (default: bdid)")
    parser.add_argument("--max-len", type=int, default=8, help="Length threshold (delete values with length < N)")
    parser.add_argument("--agg-page", type=int, default=1000, help="Composite agg page size (default: 1000)")

    # Deletion controls
    parser.add_argument("--yes", action="store_true",
                        help="Actually perform deletions without interactive prompt (global confirmation).")
    parser.add_argument("--confirm_each", action="store_true",
                        help="Ask to confirm per index before deleting.")
    parser.add_argument("--max-delete-per-index", type=int, default=50000,
                        help="Abort deletion for an index if matches exceed this number (default: 50000).")
    parser.add_argument("--print-only", action="store_true",
                        help="Only print index,bdid (no counts, no deletes). Overrides --yes.")

    args = parser.parse_args()
    es = make_client()

    indices = list_indices(es, args.pattern)
    if not indices:
        print(f"No indices found for pattern '{args.pattern}'.")
        sys.exit(0)

    # Stage 1: print index,bdid (unique list) like before
    print("index,bdid")
    per_index_data = []  # collect info for deletion phase
    for idx in sorted(indices):
        try:
            kfield = resolve_keyword_field(es, idx, args.field)
            query  = build_regex_query(kfield, args.max_len)

            # Unique bdids (for printing)
            uniq_bdids = composite_unique_values(es, idx, kfield, query, page_size=args.agg_page)
            for b in uniq_bdids:
                print(f"{idx},{b}")

            # Count docs to be deleted per index
            total_matches = count_matches(es, idx, query)
            per_index_data.append({
                "index": idx,
                "keyword_field": kfield,
                "query": query,
                "matches": total_matches,
                "uniq_count": len(uniq_bdids),
            })
        except Exception as e:
            print(f"# {idx},ERROR:{e}", file=sys.stderr)

    # If only printing, stop here
    if args.print_only:
        return

    # Stage 2: deletion (optional, guarded)
    # If no --yes and no --confirm_each, we keep it as dry-run
    if not args.yes and not args.confirm_each:
        print("\nDry-run only (no deletions). To delete, pass --yes (global) or --confirm_each (per index).")
        return

    # Global confirmation if --yes but user still wants an extra prompt:
    if args.yes and not args.confirm_each:
        # One single confirmation for all indices (type 'DELETE')
        print("\nAbout to DELETE documents where length('{field}') < {n} across indices matching '{pat}'."
              .format(field=args.field, n=args.max_len, pat=args.pattern))
        total_to_delete = sum(d["matches"] for d in per_index_data if isinstance(d.get("matches"), int))
        print(f"Total candidate docs across all indices: {total_to_delete}")
        print("Type DELETE to proceed, or press Enter to cancel:")
        typed = input("> ").strip()
        if typed != "DELETE":
            print("Canceled. No deletions performed.")
            return

    print("\n=== Deletion phase ===")
    for d in per_index_data:
        idx = d["index"]
        matches = d["matches"]
        if not isinstance(matches, int):
            print(f"{idx}: skip (unknown matches)")
            continue

        # cap check
        if matches > args.max_delete_per_index:
            print(f"{idx}: ABORT (matches {matches:,} > --max-delete-per-index {args.max_delete_per_index:,})")
            continue

        # Per-index confirmation if requested
        if args.confirm_each:
            print(f"\n{idx}: {matches} docs match (bdid length < {args.max_len}). Type index name to confirm, Enter to skip:")
            typed = input("> ").strip()
            if typed != idx:
                print(f"{idx}: skipped.")
                continue

        # If neither --yes nor per-index confirmation passed now, skip
        if not args.yes and not args.confirm_each:
            print(f"{idx}: dry-run (no delete).")
            continue

        # Perform delete_by_query
        try:
            result = delete_by_query(es, idx, d["query"])
            summary = {
                "index": idx,
                "took_ms": result.get("took"),
                "total": result.get("total"),
                "deleted": result.get("deleted"),
                "batches": result.get("batches"),
                "version_conflicts": result.get("version_conflicts"),
                "timed_out": result.get("timed_out"),
                "failures": result.get("failures"),
            }
            print(json.dumps(summary, ensure_ascii=False))
        except (AuthenticationException, TransportError, NotFoundError) as e:
            print(f"{idx}: DELETE ERROR - {e}")

    print("\nDone.")

if __name__ == "__main__":
    main()
