import os
import sys
import argparse
import warnings
from elasticsearch import Elasticsearch, ElasticsearchWarning
from elasticsearch.exceptions import AuthenticationException, NotFoundError
from dotenv import load_dotenv

def make_client():
    load_dotenv()
    hosts = os.getenv("ELASTICSEARCH_HOSTS")
    user = os.getenv("ELASTICSEARCH_USER")
    pwd  = os.getenv("ELASTICSEARCH_PASSWORD")

    if not hosts or not user or not pwd:
        print("Missing env vars: ELASTICSEARCH_HOSTS, ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD")
        sys.exit(2)

    warnings.simplefilter("ignore", ElasticsearchWarning)
    return Elasticsearch(
        hosts=[hosts],
        basic_auth=(user, pwd),
        verify_certs=False
    )

def delete_docs(es, index: str, bdid: str):
    query = {
        "query": {
            "term": { "bdid": bdid }
        }
    }
    resp = es.delete_by_query(index=index, body=query, refresh=True)
    return resp

def main():
    parser = argparse.ArgumentParser(description="Delete documents by bdid")
    parser.add_argument("-i", "--index", required=True, help="Index name or pattern")
    parser.add_argument("-b", "--bdid", required=True, help="bdid value to delete (e.g. 239356)")
    args = parser.parse_args()

    es = make_client()

    try:
        result = delete_docs(es, args.index, args.bdid)
        print("Delete by query result:")
        print(result)
    except AuthenticationException as e:
        print(f"Authentication failed: {e}")
        sys.exit(1)
    except NotFoundError:
        print(f"Index not found: {args.index}")
        sys.exit(1)
    except Exception as e:
        print(f"Error deleting documents: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
