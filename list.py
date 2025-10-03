import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

ELASTICSEARCH_HOSTS = os.getenv("ELASTICSEARCH_HOSTS")
ELASTICSEARCH_USER = os.getenv("ELASTICSEARCH_USER")
ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD")

# Connect to Elasticsearch
es = Elasticsearch(
    hosts=[ELASTICSEARCH_HOSTS],
    basic_auth=(ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD),
    verify_certs=False
)

# Get and print all indices
try:
    indices = es.indices.get_alias(index="*")
    print("List of indices:")
    for index_name in indices.keys():
        print(index_name)
except Exception as e:
    print(f"Error fetching indices: {e}")
