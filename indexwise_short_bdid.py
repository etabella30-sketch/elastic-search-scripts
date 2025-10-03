#!/usr/bin/env python3
import os
import sys
import argparse
import warnings
import json
import csv
from typing import List, Dict, Any, Tuple

from elasticsearch import Elasticsearch, ElasticsearchWarning
from elasticsearch.exceptions import AuthenticationException, NotFoundError
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

def list_indices(es: Elasticsearch, pattern: str) -> List[str]:
    # Use cat indices for explicit list
    try:
        resp = es.cat.indices(index=pattern, format="json")
        return [r["index"] for r in resp]
    except NotFoundError:
        return []
    except Exception as e:
        print(f"Error listing indices for pattern '{pattern}': {e}")
        return []

def resolve_keyword_field(es: Elasticsearch, index: str, base_field: str) -> str:
    """
    Choose a field suitable for regex/terms/composite:
      - {base_field}.keyword if exists and type=keyword
      - {base_field} if type=keyword
      Else raise RuntimeError with guidance.
    """
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

def composite_unique_bdids(
    es: Elasticsearch, index: str, keyword_field: str, query: Dict[str, Any], page_size: int = 1000
) -> List[Tuple[str, int]]:
    """
    Use composite agg to page through all unique bdids + counts.
    Returns list of (bdid, count).
    """
    results: List[Tuple[str, int]] = []
    after_key = None
    while True:
        body = {
            "size": 0,
            "query": query,
            "aggs": {
                "uniq": {
                    "composite": {
                        "size": page_size,
                        "sources": [
                            {"bdid": {"terms": {"field": keyword_field}}}
                        ]
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
            if key is not None:
                results.append((key, b.get("doc_count", 0)))
        after_key = agg.get("after_key")
        if not after_key or not buckets:
            break
    return results

def main():
    parser = argparse.ArgumentParser(
        description="Index-wise listing of docs where <field> length < N across all indices matching a pattern (default: caseindex_*)"
    )
    parser.add_argument("--pattern", default="caseindex_*", help="Index name pattern (default: caseindex_*)")
    parser.add_argument("--field", default="bdid", help="Base field name (default: bdid)")
    parser.add_argument("--max-len", type=int, default=8, help="Length threshold (default: 8)")
    parser.add_argument("--agg-page", type=int, default=1000, help="Composite page size (default: 1000)")
    parser.add_argument("--export-json", help="Export per-index unique bdids (with counts) to JSON")
    parser.add_argument("--export-csv", help="Export per-index unique bdids (with counts) to CSV")
    args = parser.parse_args()

    es = make_client()

    indices = list_indices(es, args.pattern)
    if not indices:
        print(f"No indices found for pattern '{args.pattern}'.")
        sys.exit(0)

    overall_summary = []
    export_rows = []  # for optional CSV/JSON

    for idx in sorted(indices):
        try:
            kfield = resolve_keyword_field(es, idx, args.field)
            query = build_regex_query(kfield, args.max_len)
            total = count_matches(es, idx, query)
            if total == 0:
                print(f"{idx}: matches=0, unique=0")
                overall_summary.append({"index": idx, "matches": 0, "unique": 0})
                continue

            uniq = composite_unique_bdids(es, idx, kfield, query, page_size=args.agg_page)
            uniq_count = len(uniq)
            print(f"{idx}: matches={total}, unique={uniq_count}")

            overall_summary.append({"index": idx, "matches": total, "unique": uniq_count})

            # Prepare rows for export
            for bdid, cnt in uniq:
                export_rows.append({"index": idx, "bdid": bdid, "count": cnt})

        except RuntimeError as e:
            # Mapping issue for this index — report and continue
            print(f"{idx}: SKIPPED - {e}")
            overall_summary.append({"index": idx, "matches": None, "unique": None, "error": str(e)})
        except NotFoundError:
            print(f"{idx}: SKIPPED - not found")
            overall_summary.append({"index": idx, "matches": None, "unique": None, "error": "not found"})
        except AuthenticationException as e:
            print(f"Authentication failed: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"{idx}: ERROR - {e}")
            overall_summary.append({"index": idx, "matches": None, "unique": None, "error": str(e)})

    # Print a compact final summary
    print("\n=== Summary ===")
    for row in overall_summary:
        if row.get("error"):
            print(f"{row['index']}: ERROR - {row['error']}")
        else:
            print(f"{row['index']}: matches={row['matches']}, unique={row['unique']}")

    # Optional export
    if args.export_json:
        with open(args.export_json, "w", encoding="utf-8") as f:
            json.dump(export_rows, f, ensure_ascii=False, indent=2)
        print(f"\nWrote JSON: {args.export_json}")

    if args.export_csv:
        with open(args.export_csv, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["index", "bdid", "count"])
            for r in export_rows:
                w.writerow([r["index"], r["bdid"], r["count"]])
        print(f"\nWrote CSV: {args.export_csv}")

if __name__ == "__main__":
    main()
