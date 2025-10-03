#!/usr/bin/env python3
import os
import sys
import argparse
import warnings
import json
import csv
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
            f"Field '{base_field}' is not keyword and has no 'keyword' subfield. "
            f"Add a keyword subfield or pass a keyword field."
        )
    # Field not in mapping: still try base_field.keyword (may just return 0)
    return base_field + ".keyword"

def build_regex_query(keyword_field: str, max_len: int):
    # Regex for length < max_len: .{0,max_len-1}
    n = max(0, max_len - 1)
    return {
        "bool": {
            "must": [
                {"exists": {"field": keyword_field}},
                {"regexp": {keyword_field: f".{{0,{n}}}"}}
            ]
        }
    }

def agg_unique_bdid(es: Elasticsearch, index: str, keyword_field: str, query: dict, size: int):
    # Terms agg for unique values; size = max buckets to return
    body = {
        "query": query,
        "size": 0,
        "aggs": {
            "uniq": {
                "terms": {
                    "field": keyword_field,
                    "size": size,          # increase if you expect more unique values
                    "order": {"_key": "asc"}
                }
            }
        }
    }
    resp = es.search(index=index, body=body)
    buckets = resp.get("aggregations", {}).get("uniq", {}).get("buckets", [])
    return [{"bdid": b["key"], "count": b["doc_count"]} for b in buckets]

def main():
    parser = argparse.ArgumentParser(description="List UNIQUE bdid values with length < N (no repeats).")
    parser.add_argument("-i", "--index", required=True, help="Index name")
    parser.add_argument("--field", default="bdid", help="Base field name (default: bdid)")
    parser.add_argument("--max-len", type=int, default=8, help="Length threshold (default: 8)")
    parser.add_argument("--agg-size", type=int, default=10000, help="Max unique values to return (default: 10000)")
    parser.add_argument("--with-count", action="store_true", help="Print each unique bdid with its doc count")
    parser.add_argument("--export-json", help="Export unique bdids to JSON file")
    parser.add_argument("--export-csv", help="Export unique bdids to CSV file")
    args = parser.parse_args()

    es = make_client()

    try:
        kfield = resolve_keyword_field(es, args.index, args.field)
        query = build_regex_query(kfield, args.max_len)

        uniques = agg_unique_bdid(es, args.index, kfield, query, args.agg_size)
        total_unique = len(uniques)
        print(f"Total unique {args.field} with length < {args.max_len}: {total_unique}")

        # Print to stdout
        for u in uniques:
            if args.with_count:
                print(json.dumps(u, ensure_ascii=False))
            else:
                print(u["bdid"])

        # Optional export
        if args.export_json:
            with open(args.export_json, "w", encoding="utf-8") as f:
                json.dump(uniques if args.with_count else [u["bdid"] for u in uniques],
                          f, ensure_ascii=False, indent=2)
            print(f"Wrote JSON: {args.export_json}")

        if args.export_csv:
            with open(args.export_csv, "w", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                if args.with_count:
                    w.writerow(["bdid", "count"])
                    for u in uniques:
                        w.writerow([u["bdid"], u["count"]])
                else:
                    w.writerow(["bdid"])
                    for u in uniques:
                        w.writerow([u["bdid"]])
            print(f"Wrote CSV: {args.export_csv}")

    except AuthenticationException as e:
        print(f"Authentication failed: {e}")
        sys.exit(1)
    except NotFoundError:
        print(f"Index not found: {args.index}")
        sys.exit(1)
    except RuntimeError as e:
        print(f"Error: {e}")
        sys.exit(2)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
