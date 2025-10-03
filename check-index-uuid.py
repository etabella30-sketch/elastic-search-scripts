import os
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, TransportError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def list_case_indices():
    es_host = os.getenv("ELASTICSEARCH_HOSTS")
    es_user = os.getenv("ELASTICSEARCH_USER")
    es_password = os.getenv("ELASTICSEARCH_PASSWORD")

    if not es_host or not es_user or not es_password:
        print("Missing Elasticsearch credentials in .env file")
        return

    client = Elasticsearch(
        es_host,
        basic_auth=(es_user, es_password),
        verify_certs=False  # Set to True if HTTPS with valid cert
    )

    try:
        indices = client.indices.get(index="caseindex_*")
        print("Matched Indices:")
        for index_name in indices:
            print(index_name)
    except (ConnectionError, TransportError) as e:
        print(f"Elasticsearch error: {e}")

if __name__ == "__main__":
    list_case_indices()
