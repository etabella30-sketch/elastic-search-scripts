import os
import sys
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import AuthenticationException
from dotenv import load_dotenv

def make_client():
    load_dotenv()
    hosts = os.getenv("ELASTICSEARCH_HOSTS")
    user  = os.getenv("ELASTICSEARCH_USER")
    pwd   = os.getenv("ELASTICSEARCH_PASSWORD")
    if not hosts or not user or not pwd:
        print("Missing env vars: ELASTICSEARCH_HOSTS, ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD")
        sys.exit(2)

    return Elasticsearch(hosts=[hosts], basic_auth=(user, pwd), verify_certs=False)

def get_index_summary(es):
    # Use cat indices API for quick human-friendly stats
    resp = es.cat.indices(format="json", bytes="b")  # bytes="b" => size in bytes
    return resp

def main():
    es = make_client()
    try:
        data = get_index_summary(es)
        print(f"{'Index':50} {'Docs':>15} {'Size (MB)':>15}")
        print("-" * 85)
        for idx in data:
            index_name = idx.get("index", "")
            docs_count = int(idx.get("docs.count", 0))
            store_size_bytes = int(idx.get("store.size", 0))
            size_mb = store_size_bytes / (1024 * 1024)
            print(f"{index_name:50} {docs_count:15,} {size_mb:15,.2f}")
    except AuthenticationException as e:
        print(f"Authentication failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error fetching index summary: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
