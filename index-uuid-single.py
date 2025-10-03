import boto3
import redis
import psycopg2
import elasticsearch
from datetime import datetime
import logging
from dotenv import load_dotenv
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, TimeoutError
from botocore.config import Config
import time

# Configure the main logger for console output
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a dedicated logger for failed files, writing to "failed_files.log"
failed_logger = logging.getLogger("failed_logger")
failed_logger.setLevel(logging.ERROR)
fh = logging.FileHandler("failed_files.log")
fh.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
failed_logger.addHandler(fh)

def load_config():
    """Load configuration from environment variables"""
    load_dotenv()
    
    # PostgreSQL Configuration
    pg_conn_string = (
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )
    
    # Redis Configuration
    redis_conn = {
        "host": os.getenv('REDIS_HOST'),
        "port": int(os.getenv('REDIS_PORT', 6379)),
        "password": os.getenv('REDIS_PASSWORD'),
        "decode_responses": True
    }
    
    # Elasticsearch Configuration
    es_conn = {
        "hosts": os.getenv('ELASTICSEARCH_HOSTS', 'http://localhost:9200').split(','), 
        "basic_auth": (
            os.getenv('ELASTICSEARCH_USER'),
            os.getenv('ELASTICSEARCH_PASSWORD')
        ) if os.getenv('ELASTICSEARCH_USER') else None
    }
    
    # S3 Configuration
    s3_config = {
        "aws_access_key_id": os.getenv('AWS_ACCESS_KEY_ID'),
        "aws_secret_access_key": os.getenv('AWS_SECRET_ACCESS_KEY'),
        "region_name": os.getenv('AWS_REGION'),
        "endpoint_url": os.getenv('AWS_ENDPOINT_URL'),
        "config": Config(max_pool_connections=50)
    }
    
    # Batch Sizes (default values if not set)
    batch_size = int(os.getenv('BATCH_SIZE', 4))
    doc_batch_size = int(os.getenv('DOC_BATCH_SIZE', 5))
    page_batch_size = int(os.getenv('PAGE_BATCH_SIZE', 5))
    print(f"Download batch: {doc_batch_size}")
    return pg_conn_string, redis_conn, es_conn, s3_config, batch_size, doc_batch_size, page_batch_size

def safe_download_file(bucket, key, local_file_path):
    """
    Creates its own boto3 client and downloads the file.
    Runs in a separate process so that any segfault inside native code
    will not crash the main process.
    """
    import boto3
    from botocore.config import Config
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION'),
        endpoint_url=os.getenv('AWS_ENDPOINT_URL'),
        config=Config(max_pool_connections=50)
    )
    s3_client.download_file(bucket, key, local_file_path)

def safe_process_pdf(file_path):
    """
    Process PDF content in a separate process by opening it directly from disk.
    Returns a list of pages with page number and text.
    """
    import fitz  # PyMuPDF
    pages = []
    try:
        doc = fitz.open(file_path)
    except Exception as e:
        raise Exception(f"Error opening PDF: {e}")
    total_pages = len(doc)
    for page_num in range(total_pages):
        try:
            page = doc[page_num]
            text = page.get_text()
            pages.append({'pg': page_num + 1, 'ct': text})
        except Exception:
            continue
    doc.close()
    return pages

class DocumentProcessor:
    def __init__(self, pg_conn_string, redis_conn, es_conn, s3_config, batch_size, doc_batch_size, page_batch_size):
        self.pg_conn = psycopg2.connect(pg_conn_string)
        self.redis_client = redis.Redis(**redis_conn)
        self.es_client = elasticsearch.Elasticsearch(**es_conn)
        # Use persistent process pools for downloads and PDF processing
        self.download_pool = ProcessPoolExecutor(max_workers=2)
        self.pdf_pool = ProcessPoolExecutor(max_workers=2)
        self.batch_size = batch_size
        self.doc_batch_size = doc_batch_size
        self.page_batch_size = page_batch_size

    def filter_page_content(self, text):
        if not text:
            return text
        return text.replace('-\n', '\n')
    
    def get_pending_cases(self):
        cases = self.redis_client.hgetall("elasticsingle:cases")
        return [(case_id) for case_id, status in cases.items() if status == 'P']

    def update_case_status(self, case_id, status):
        self.redis_client.hset("elasticsingle:cases", case_id, status)

    # def get_case_documents(self, case_id):
        # with self.pg_conn.cursor() as cursor:
        #     cursor.execute("""
        #         SELECT s."nCaseid" cid, 
        #                s."nSectionid" sid,
        #                COALESCE(d."nBundleid", 0) bid,
        #                d."nBundledetailid" as bdid,
        #                d."cPath" as path
        #         FROM "BundleDetail" d 
        #         JOIN "SectionMaster" s ON s."nSectionid" = d."nSectionid"
        #         WHERE d."cFiletype" = 'PDF' 
        #           AND s."nCaseid" = %s 
        #           AND d."cStatus" = 'C'
        #         ORDER BY "nBundledetailid"
        #     """, (case_id,))
        #     columns = ['cid', 'sid', 'bid', 'bdid', 'path']
        #     results = cursor.fetchall()
        #     return [dict(zip(columns, row)) for row in results]

    def get_case_documents(self, case_id):
        with self.pg_conn.cursor() as cursor:
            cursor.execute("""
                SELECT s."nCaseid" cid, 
                       s."nSectionid" sid,
                       COALESCE(d."nBundleid"::text, '') bid,
                       d."nBundledetailid" as bdid,
                       d."cPath" as path
                FROM "BundleDetail" d 
                JOIN "SectionMaster" s ON s."nSectionid" = d."nSectionid"
                WHERE d."cFiletype" = 'PDF' 
                  and d."nBundledetailid" = 'c83dee70-2df4-4395-b93e-8b03f3e5f50d'
                  AND d."cStatus" = 'C'
                ORDER BY "nBundledetailid"
            """, (case_id,))
            columns = ['cid', 'sid', 'bid', 'bdid', 'path']
            results = cursor.fetchall()
            documents = [dict(zip(columns, row)) for row in results]

            # Filter out documents that are already indexed
            # This extra step could be removed if performance becomes an issue
            filtered_documents = []
            for doc in documents:
                if not self.is_document_indexed(case_id, doc['bdid']):
                    filtered_documents.append(doc)
                else:
                    logger.info(f"Skipping already indexed document with bdid {doc['bdid']}")
                    # Update Redis to reflect that it's already done
                    self.update_processing_status(doc['cid'], doc['bdid'], 'completed')

            return filtered_documents


    def process_pdf_content(self, file_path):
        """
        Process the PDF in an isolated process using a persistent process pool.
        """
        future = self.pdf_pool.submit(safe_process_pdf, file_path)
        try:
            # Wait up to 120 seconds for PDF processing.
            pages = future.result(timeout=120)
        except Exception as e:
            raise Exception(f"PDF processing failed: {e}")
        return pages

    def initialize_redis_tracking(self, case_id, total_docs):
        key = f"single:case:{case_id}:summary"
        if not self.redis_client.exists(key):
            pipe = self.redis_client.pipeline()
            pipe.hset(key, mapping={"total": total_docs, "completed": 0, "failed": 0})
            pipe.execute()
        else:
            self.redis_client.hset(key, "total", total_docs)

    def update_processing_status(self, case_id, bdid, status):
        timestamp = datetime.now().isoformat()
        pipe = self.redis_client.pipeline()
        key = f"elasticsingle:{case_id}:{status}"
        pipe.hset(key, bdid, timestamp)
        pipe.hincrby(f"single:case:{case_id}:summary", status, 1)
        pipe.execute()

    def index_document(self, doc_info, page_data):
        doc = {
            'cid': doc_info['cid'],
            'sid': doc_info['sid'],
            'bid': doc_info['bid'],
            'bdid': doc_info['bdid'],
            'ct': page_data['ct'],
            'pg': page_data['pg']
        }
        document_id = f"{doc_info['bdid']}_{page_data['pg']}"
        try:
            self.es_client.update(
                index=f"caseindex_{doc_info['cid']}",
                id=document_id,
                doc=doc,
                doc_as_upsert=True
            )
            logger.info(f"Successfully indexed document {document_id} for case {doc_info['cid']}")
        except Exception as e:
            logger.error(f"Error indexing document {document_id}: {str(e)}")
            raise

    def ensure_case_directory(self, case_id):
        case_dir = os.path.join('docs', str(case_id))
        os.makedirs(case_dir, exist_ok=True)
        return case_dir

    def cleanup_case_directory(self, case_dir):
        if os.path.exists(case_dir):
            for file in os.listdir(case_dir):
                os.remove(os.path.join(case_dir, file))
            os.rmdir(case_dir)

    def download_file_with_retry(self, doc, local_file_path):
        bucket = os.getenv('AWS_BUCKET')
        key = doc['path']
        max_retries = int(os.getenv('MAX_DOWNLOAD_RETRIES', 3))
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting download for bdid {doc['bdid']}, attempt {attempt + 1}/{max_retries}")
                future = self.download_pool.submit(safe_download_file, bucket, key, local_file_path)
                future.result(timeout=120)
                logger.info(f"Successfully downloaded file: {doc['path']}")
                return True
            except Exception as e:
                logger.warning(f"Download attempt {attempt+1} failed, retrying... Error: {e}")
                if attempt == max_retries - 1:
                    err_msg = f"Failed to download file after {max_retries} attempts: {e}"
                    logger.error(err_msg)
                    failed_logger.error(f"Download failure for document {doc['bdid']} (Path: {doc['path']}): {e}")
                    return False
        return False

    def ensure_index_exists(self, case_id):
        index_name = f"caseindex_{case_id}"
        try:
            if not self.es_client.indices.exists(index=index_name):
                self.es_client.indices.create(
                    index=index_name,
                    body={
                        "settings": {
                            "analysis": {
                                "analyzer": {
                                    "word_boundary_analyzer": {
                                        "type": "custom",
                                        "tokenizer": "standard",
                                        "filter": ["lowercase", "word_delimiter_graph"]
                                    },
                                    "case_sensitive": {
                                        "type": "custom",
                                        "tokenizer": "standard",
                                        "filter": []
                                    },
                                    "prefix_analyzer": {
                                        "type": "custom",
                                        "tokenizer": "standard",
                                        "filter": ["lowercase", "edge_ngram_filter"]
                                    }
                                },
                                "filter": {
                                    "edge_ngram_filter": {
                                        "type": "edge_ngram",
                                        "min_gram": 1,
                                        "max_gram": 20
                                    }
                                },
                                "normalizer": {
                                    "lowercase_normalizer": {
                                        "type": "custom",
                                        "filter": ["lowercase"]
                                    }
                                }
                            }
                        },
                        "mappings": {
                            "properties": {
                                "cid": {"type": "keyword"},
                                "sid": {"type": "keyword"},
                                "bid": {"type": "keyword"},
                                "bdid": {"type": "keyword"},
                                "pg": {"type": "integer"},
                                "ct": {
                                    "type": "text",
                                    "analyzer": "standard",
                                    "fields": {
                                        "word_boundary": {"type": "text", "analyzer": "word_boundary_analyzer"},
                                        "raw": {"type": "text", "analyzer": "case_sensitive"},
                                        "keyword": {"type": "keyword"},
                                        "keyword_lower": {"type": "keyword", "normalizer": "lowercase_normalizer"},
                                        "prefix": {"type": "text", "analyzer": "prefix_analyzer"},
                                        "prefix_raw": {"type": "text", "analyzer": "case_sensitive"}
                                    }
                                }
                            }
                        }
                    }
                )
                logger.info(f"Created new index: {index_name}")
            return True
        except Exception as e:
            logger.error(f"Error ensuring index exists: {e}")
            return False

    def delete_existing_document_pages(self, doc_info):
        try:
            if not self.ensure_index_exists(doc_info['cid']):
                raise Exception("Failed to create index")
            self.es_client.delete_by_query(
                index=f"caseindex_{doc_info['cid']}",
                body={"query": {"term": {"bdid": doc_info['bdid']}}}
            )
            logger.info(f"Successfully deleted existing pages for bdid {doc_info['bdid']}")
        except Exception as e:
            logger.error(f"Error deleting existing pages for bdid {doc_info['bdid']}: {e}")
            failed_logger.error(f"Deletion failure for document {doc_info['bdid']} (Case {doc_info['cid']}): {e}")
            raise

    def is_document_indexed(self, case_id, bdid):
        """Check if a document is already indexed in Elasticsearch"""
        try:
            # A light query to check if at least one page exists for this bdid
            query = {
                "size": 0,  # We only need the count, not the actual documents
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"cid": case_id}},
                            {"term": {"bdid": bdid}}
                        ]
                    }
                }
            }
    
            response = self.es_client.search(
                index=f"caseindex_{case_id}",
                body=query
            )
    
            # If hits exist, document is already indexed
            return response["hits"]["total"]["value"] > 0
        except Exception as e:
            logger.warning(f"Error checking if document {bdid} is indexed: {e}")
            # If error occurs during check, assume not indexed to be safe
            return False



    def process_case(self, case_id):
        case_dir = None
        process_start_time = datetime.now().isoformat()
        try:
            documents = self.get_case_documents(case_id)
            self.initialize_redis_tracking(case_id, len(documents))
            self.redis_client.hset(f"single:case:{case_id}:summary", "processStart", process_start_time)
            completed_docs = set(self.redis_client.hkeys(f"elasticsingle:{case_id}:completed"))
            logger.info(f"Completed documents for case {case_id}: {completed_docs}")
            documents_to_process = [doc for doc in documents if str(doc['bdid']) not in completed_docs]
            logger.info(f"Processing {len(documents_to_process)} out of {len(documents)} documents for case {case_id}")
            case_dir = self.ensure_case_directory(case_id)





            def process_document(doc):
                local_file_path = os.path.join(case_dir, f"{doc['bdid']}.pdf")
                try:
                    if self.redis_client.hexists(f"elasticsingle:{case_id}:completed", doc['bdid']):
                        logger.info(f"Skipping document {doc['bdid']} as it is already completed")
                        return
                    
                    # NEW CHECK: Skip if document is already in Elasticsearch
                    if self.is_document_indexed(doc['cid'], doc['bdid']):
                        logger.info(f"Skipping document {doc['bdid']} as it is already indexed in Elasticsearch")
                        # Update Redis to mark as completed since it's already in ES
                        self.update_processing_status(doc['cid'], doc['bdid'], 'completed')
                        return
        
                    
                    if not self.download_file_with_retry(doc, local_file_path):
                        logger.error(f"Failed to download document {doc['bdid']}, marking as failed")
                        self.update_processing_status(doc['cid'], doc['bdid'], 'failed')
                        return
                    # Process PDF directly from disk instead of reading the entire file into memory
                    pages = self.process_pdf_content(local_file_path)
                    self.delete_existing_document_pages(doc)
                    with ThreadPoolExecutor(max_workers=self.page_batch_size) as page_executor:
                        page_executor.map(self.index_document, [doc] * len(pages), pages)
                    self.update_processing_status(doc['cid'], doc['bdid'], 'completed')
                    os.remove(local_file_path)
                except Exception as e:
                    err = f"Error processing document {doc['bdid']} (Path: {doc['path']}): {e}"
                    logger.error(err)
                    failed_logger.error(err)
                    self.update_processing_status(doc['cid'], doc['bdid'], 'failed')
                    if os.path.exists(local_file_path):
                        os.remove(local_file_path)

            with ThreadPoolExecutor(max_workers=self.doc_batch_size) as file_executor:
                file_executor.map(process_document, documents_to_process)
        except Exception as e:
            logger.error(f"Error processing case {case_id}: {e}")
            raise
        finally:
            if case_dir and os.path.exists(case_dir):
                self.cleanup_case_directory(case_dir)
            process_end_time = datetime.now().isoformat()
            self.redis_client.hset(f"single:case:{case_id}:summary", "processEnd", process_end_time)
            logger.info(f"Processing completed for case {case_id} (Start: {process_start_time}, End: {process_end_time})")

    def process_all_cases(self):
        pending_cases = self.get_pending_cases()
        logger.info(f"Found {len(pending_cases)} pending cases to process in batches of {self.batch_size}")
        with ThreadPoolExecutor(max_workers=self.batch_size) as executor:
            future_to_case = {executor.submit(self.process_case, case_id): case_id for case_id in pending_cases}
            for future in future_to_case:
                case_id = future_to_case[future]
                try:
                    future.result()
                    self.update_case_status(case_id, 'C')
                    logger.info(f"Successfully completed processing case {case_id}")
                except Exception as e:
                    logger.error(f"Failed to process case {case_id}: {e}")
                    self.update_case_status(case_id, 'F')
        logger.info("Completed processing all cases.")

def main():
    try:
        pg_conn_string, redis_conn, es_conn, s3_config, batch_size, doc_batch_size, page_batch_size = load_config()
        processor = DocumentProcessor(pg_conn_string, redis_conn, es_conn, s3_config, batch_size, doc_batch_size, page_batch_size)
        processor.process_all_cases()
    except Exception as e:
        logger.error(f"Main execution error: {e}")
        raise

def run_continuously():
    while True:
        try:
            main()
        except Exception as e:
            logger.error(f"An error occurred in main loop: {e}")
        time.sleep(10)

if __name__ == "__main__":
    run_continuously()
