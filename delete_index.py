import os
import sys
import argparse
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, AuthenticationException
from dotenv import load_dotenv

def make_client():
    load_dotenv()
    hosts = os.getenv("ELASTICSEARCH_HOSTS")
    user  = os.getenv("ELASTICSEARCH_USER")
    pwd   = os.getenv("ELASTICSEARCH_PASSWORD")
    if not hosts or not user or not pwd:
        print("Missing env vars: ELASTICSEARCH_HOSTS, ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD")
        sys.exit(2)

    return Elasticsearch(
        hosts=[hosts],
        basic_auth=(user, pwd),
        verify_certs=False
    )

def delete_index(es, index_name):
    if not es.indices.exists(index=index_name).body:
        print(f"Index not found: {index_name}")
        sys.exit(1)
    es.indices.delete(index=index_name)
    print(f"Deleted index: {index_name}")

def main():
    parser = argparse.ArgumentParser(description="Delete an Elasticsearch index by name.")
    parser.add_argument("-i", "--index", required=True, help="Index name to delete")
    parser.add_argument("--force", action="store_true", help="Skip confirmation prompt")
    args = parser.parse_args()

    es = make_client()

    try:
        if not args.force:
            confirm = input(f"Are you sure you want to delete index '{args.index}'? (y/N): ").strip().lower()
            if confirm != "y":
                print("Aborted.")
                sys.exit(0)
        delete_index(es, args.index)

    except AuthenticationException:
        print("Authentication failed — check your credentials.")
        sys.exit(1)
    except NotFoundError:
        print(f"Index not found: {args.index}")
        sys.exit(1)
    except Exception as e:
        print(f"Error deleting index: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
