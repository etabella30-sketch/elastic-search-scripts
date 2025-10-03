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

def resolve_keyword_field(es: Elasticsearch, index: str, base_field: str) -> str:
    """
    Return a field name suitable for regex/term queries:
      1) base_field.keyword if exists and type=keyword
      2) base_field if type=keyword
      Else: raise with guidance.
    """
    m = es.indices.get_mapping(index=index)
    m = getattr(m, "body", m)
    # If index is an alias to many indices, just check the first mapping we see.
    _, idx_map = next(iter(m.items()))
    props = idx_map.get("mappings", {}).get("properties", {})

    if base_field in props:
        fld = props[base_field]
        # Prefer subfield keyword
        sub = fld.get("fields", {})
        if "keyword" in sub and sub["keyword"].get("type") == "keyword":
            return f"{base_field}.keyword"
        # Or the base field itself is keyword
        if fld.get("type") == "keyword":
            return base_field
        # Else it's probably text
        raise RuntimeError(
            f"Field '{base_field}' is not keyword (likely 'text') and has no 'keyword' subfield. "
            f"Regex/length queries won’t work on analyzed text. "
            f"Options: add a keyword subfield, reindex with a normalizer, or use a newer cluster feature."
        )
    else:
        # Field not in mapping—still allow the query (will return 0), but warn.
        print(f"Warning: field '{base_field}' not found in mapping; proceeding (likely 0 matches).")
        return base_field + ".keyword"  # best guess

def build_regex_body(field: str, max_len: int, size: int):
    """
    Lucene regex for length < max_len:
      .{0,N} matches up to N chars; we want < max_len, so N = max_len - 1
    Require field existence to avoid null/empty edge cases.
    """
    pattern_max = max_len - 1
    if pattern_max < 0:
        pattern_max = 0
    body = {
        "query": {
            "bool": {
                "must": [
                    {"exists": {"field": field}},
                    {"regexp": {field: f".{{0,{pattern_max}}}"}}
                ]
            }
        },
        "_source": [field],
        "size": size,
        "sort": ["_doc"]
    }
    return body

def count_with_same_query(es, index: str, body: dict) -> int:
    count_body = {
        "query": body["query"]
    }
    resp = es.count(index=index, body=count_body)
    return resp.get("count", 0)

def main():
    parser = argparse.ArgumentParser(description="List docs where bdid length is < N using a regex on keyword field.")
    parser.add_argument("-i", "--index", required=True, help="Index name")
    parser.add_argument("--field", default="bdid", help="Base field name (default: bdid)")
    parser.add_argument("--max-len", type=int, default=8, help="Match values with length < N (default: 8)")
    parser.add_argument("--size", type=int, default=20, help="Preview size (default: 20)")
    args = parser.parse_args()

    es = make_client()

    try:
        keyword_field = resolve_keyword_field(es, args.index, args.field)
        body = build_regex_body(keyword_field, args.max_len, args.size)

        total = count_with_same_query(es, args.index, body)
        print(f"Total matching docs (length of '{keyword_field}' < {args.max_len}): {total}")

        if args.size > 0:
            resp = es.search(index=args.index, body=body)
            hits = resp.get("hits", {}).get("hits", [])
            if not hits:
                print("No preview docs to show.")
            else:
                print(f"\nShowing up to {args.size} sample docs:")
                for h in hits:
                    src = h.get("_source", {})
                    print(json.dumps({
                        "_id": h.get("_id"),
                        args.field: src.get(args.field)
                    }, ensure_ascii=False))

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
