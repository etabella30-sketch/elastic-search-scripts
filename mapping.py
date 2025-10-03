import os
import sys
import json
import argparse
import warnings
from elasticsearch import Elasticsearch, ElasticsearchWarning
from elasticsearch.exceptions import NotFoundError, AuthenticationException
from dotenv import load_dotenv

def make_client():
    load_dotenv()
    hosts = os.getenv("ELASTICSEARCH_HOSTS")
    user = os.getenv("ELASTICSEARCH_USER")
    pwd  = os.getenv("ELASTICSEARCH_PASSWORD")

    if not hosts or not user or not pwd:
        print("Missing env vars: ELASTICSEARCH_HOSTS, ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD")
        sys.exit(2)

    warnings.simplefilter("ignore", ElasticsearchWarning)  # silence system-index warnings if desired

    return Elasticsearch(
        hosts=[hosts],
        basic_auth=(user, pwd),
        verify_certs=False
    )

def get_mapping(es, index_or_pattern: str):
    resp = es.indices.get_mapping(index=index_or_pattern)
    # In ES 8.x this is ObjectApiResponse; use .body to get the dict
    return getattr(resp, "body", resp)

def main():
    parser = argparse.ArgumentParser(description="Print Elasticsearch index mapping.")
    parser.add_argument("-i", "--index", required=True,
                        help="Index name or pattern (e.g. case_documents_1051 or case_documents_*)")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    parser.add_argument("--out", help="Write output to file")
    args = parser.parse_args()

    es = make_client()

    try:
        mappings = get_mapping(es, args.index)
        if not mappings:
            print(f"No mapping found for: {args.index}")
            sys.exit(1)

        output = json.dumps(mappings, indent=2 if args.pretty else None, ensure_ascii=False)

        if args.out:
            with open(args.out, "w", encoding="utf-8") as f:
                f.write(output)
            print(f"Wrote mapping to {args.out}")
        else:
            print(output)

    except AuthenticationException as e:
        print(f"Authentication failed: {e}")
        sys.exit(1)
    except NotFoundError:
        print(f"Index not found: {args.index}")
        sys.exit(1)
    except Exception as e:
        print(f"Error fetching mapping: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
