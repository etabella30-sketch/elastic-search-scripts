#!/usr/bin/env python3
import os
import sys
import argparse
import warnings
import json
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

def list_indices(es: Elasticsearch, pattern: str):
    resp = es.cat.indices(index=pattern, format="json")
    return [r["index"] for r in resp]

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
        raise RuntimeError(f"Field '{base_field}' is not keyword in index {index}")
    raise RuntimeError(f"Field '{base_field}' not found in {index}")

def build_regex_query(field: str, max_len: int):
    n = max(0, max_len - 1)
    return {
        "bool": {
            "must": [
                {"exists": {"field": field}},
                {"regexp": {field: f".{{0,{n}}}"}}
            ]
        }
    }

def agg_unique(es: Elasticsearch, index: str, field: str, query: dict, page_size: int = 1000):
    results = []
    after_key = None
    while True:
        body = {
            "size": 0,
            "query": query,
            "aggs": {
                "uniq": {
                    "composite": {
                        "size": page_size,
                        "sources": [{"bdid": {"terms": {"field": field}}}]
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
                results.append(key)
        after_key = agg.get("after_key")
        if not after_key or not buckets:
            break
    return results

def main():
    parser = argparse.ArgumentParser(description="List unique bdids (< N chars) per index with index name")
    parser.add_argument("--pattern", default="caseindex_*", help="Index name pattern (default: caseindex_*)")
    parser.add_argument("--field", default="bdid", help="Base field name (default: bdid)")
    parser.add_argument("--max-len", type=int, default=8, help="Length threshold (default: 8)")
    args = parser.parse_args()

    es = make_client()
    indices = list_indices(es, args.pattern)
    if not indices:
        print("No matching indices found")
        sys.exit(0)

    print("index,bdid")  # CSV-like header
    for idx in sorted(indices):
        try:
            kfield = resolve_keyword_field(es, idx, args.field)
            query = build_regex_query(kfield, args.max_len)
            uniq_bdids = agg_unique(es, idx, kfield, query)
            for bdid in uniq_bdids:
                print(f"{idx},{bdid}")
        except Exception as e:
            print(f"# {idx},ERROR:{e}", file=sys.stderr)

if __name__ == "__main__":
    main()
