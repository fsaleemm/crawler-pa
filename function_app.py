import logging
import azure.functions as func

import azure.functions as func
import logging
from selenium.webdriver.common.by import By
from crawler.WebCrawler import WebCrawler
from chunker.text_chunker import chunk_file_content
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents import SearchClient
from indexer.search_manager import create_search_index
from indexer.search_manager import upload_documents_to_index

import queue, threading
import uuid, base64, os, time, requests, json
from datetime import datetime

from azure.cosmos import CosmosClient, PartitionKey
import hashlib

app = func.FunctionApp()

@app.schedule(schedule="0 0 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=True) 
def timercrawler(myTimer: func.TimerRequest) -> None:
    
    cosmos_logger = os.environ.get("COSMOS_Logger", "true")
    cosmos_logger_bool = cosmos_logger.lower() in ['true', '1']

    # Generate a unique run ID
    run_id = str(uuid.uuid4())

    logger = get_cosmosdb_logger(run_id) if cosmos_logger_bool else logging.getLogger()

    if myTimer.past_due:
        logger.info('The timer is past due!')

    start_time = time.time()
    formatted_start_time = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"Starting crawler: {formatted_start_time}")

    # Do Something
    orchestrator = Orchestrator(logger)
    orchestrator.orchestrate()

    end_time = time.time()
    formatted_end_time = datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Ending crawler: {formatted_end_time}")

    elapsed_time = end_time - start_time
    logger.info("Elapsed time: {:.2f} seconds".format(elapsed_time))

# Custom Logging Handler
class CosmosDBHandler(logging.Handler):
    def __init__(self, cosmos_url, cosmos_key, database_name, container_name, run_id):
        super().__init__()
        self.client = CosmosClient(cosmos_url, credential=cosmos_key)
        self.database = self.client.create_database_if_not_exists(database_name)
        self.container = self.database.create_container_if_not_exists(
            id=container_name,
            partition_key=PartitionKey(path="/level"),
        )
        self.formatter = logging.Formatter()
        self.run_id = run_id

    def emit(self, record):
        record.asctime = self.formatter.formatTime(record)
        log_entry = {
            "id": str(uuid.uuid4()),
            "time": record.asctime,
            "run_id": self.run_id,
            "name": record.name,
            "level": record.levelname,
            "message": record.getMessage(),  # This will format the message
        }
        self.container.upsert_item(body=log_entry)

def get_cosmosdb_logger(run_id):
    # Create a logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Create a CosmosDB handler
    cosmos_handler = CosmosDBHandler(
        cosmos_url = os.environ.get("COSMOS_URL"),
        cosmos_key = os.environ.get("COSMOS_DB_KEY"),
        database_name = os.environ.get("COSMOS_DATABASE_NAME", "CrawlStore"),
        container_name = "CrawlerLog",
        run_id = run_id
    )

    # Use JSON formatter
    formatter = logging.Formatter('%(message)s')
    cosmos_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(cosmos_handler)

    return logger

class Orchestrator:
    _shared_state = {}

    def __init__(self, logger):
        self.__dict__ = self._shared_state

        if not self._shared_state:
            self.logging = logger
            self.NUM_OF_THREADS = int(os.getenv("NUM_OF_THREADS", 1))
            self.EXCLUDE_LIST = os.getenv('EXCLUDE_LIST', "").split(',')
            self.BASE_URLS = os.getenv('BASE_URLS', "").split(',')
            self.EXTRACT_LINK_TYPE = [file_type.lower() for file_type in os.getenv('EXTRACT_LINK_TYPE', "").split(',')]
            self.CRAWL_URLS = os.getenv('CRAWL_URLS', "").split(',')

            enable_vectors_str = os.getenv("ENABLE_VECTORS", "false")
            self.ENABLE_VECTORS = enable_vectors_str.lower() in ['true', '1', 'yes']

            self.INDEX_NAME = os.getenv("INDEX_NAME", "crawler-index")
            self.SEARCH_ENDPOINT = os.getenv("SEARCH_ENDPOINT")
            self.SEARCH_CREDS = AzureKeyCredential(os.getenv("SEARCH_KEY"))
            self.FORM_RECOGNIZER_ENDPOINT = os.getenv("FORM_RECOGNIZER_ENDPOINT") 
            self.FORM_RECOGNIZER_CREDS = AzureKeyCredential(os.getenv("FORM_RECOGNIZER_KEY"))
            self.COSMOS_URL = os.environ.get("COSMOS_URL")
            self.COSMOS_KEY = os.environ.get("COSMOS_DB_KEY")
            self.DATABASE_NAME = os.environ.get("COSMOS_DATABASE_NAME", "CrawlStore")
            self.CONTAINER_NAME = os.environ.get("COSMOS_CONTAINER_NAME", "URLChangeLog")

            self.index_client, self.search_client, self.form_recognizer_client, self.cosmosdb_client, self.database, self.container = self.setup_clients()
            self.crawler_store_items = self.setup_crawler_data()

        self.logging.info(f"NUM_OF_THREADS: {self.NUM_OF_THREADS}")
        self.logging.info(f"EXCLUDE_LIST: {self.EXCLUDE_LIST}")
        self.logging.info(f"BASE_URLS: {self.BASE_URLS}")
        self.logging.info(f"EXTRACT_LINK_TYPE: {self.EXTRACT_LINK_TYPE}")
        self.logging.info(f"CRAWL_URLS: {self.CRAWL_URLS}")
        self.logging.info(f"ENABLE_VECTORS: {self.ENABLE_VECTORS}")
        self.logging.info(f"INDEX_NAME: {self.INDEX_NAME}")
        self.logging.info(f"SEARCH_ENDPOINT: {self.SEARCH_ENDPOINT}")
        self.logging.info(f"FORM_RECOGNIZER_ENDPOINT: {self.FORM_RECOGNIZER_ENDPOINT}")
        self.logging.info(f"COSMOS_URL: {self.COSMOS_URL}")
        self.logging.info(f"COSMOS_DATABASE_NAME: {self.DATABASE_NAME}")
        self.logging.info(f"COSMOS_CONTAINER_NAME: {self.CONTAINER_NAME}")


    def setup_clients(self):
        # Azure Search
        
       
        index_client = SearchIndexClient(endpoint=self.SEARCH_ENDPOINT, credential=self.SEARCH_CREDS)
        search_client = SearchClient(
            endpoint=self.SEARCH_ENDPOINT, credential=self.SEARCH_CREDS, index_name=self.INDEX_NAME
        )

        # Azure Form Recognizer
        form_recognizer_client = DocumentAnalysisClient(
            endpoint=self.FORM_RECOGNIZER_ENDPOINT,
            credential=self.FORM_RECOGNIZER_CREDS,
        )

        # Azure CosmosDB Client
        
        cosmosdb_client = CosmosClient(self.COSMOS_URL, credential=self.COSMOS_KEY)
        # Create the database if it does not exist
        self.logging.info(f"Creating Cosmos DB if it does not exist: {self.DATABASE_NAME}")
        database = cosmosdb_client.create_database_if_not_exists(id=self.DATABASE_NAME)

        # Define a partition key for the container
        partition_key = PartitionKey(path="/url")

        # Create the container if it does not exist
        self.logging.info(f"Creating Cosmos DB container if it does not exist: {self.CONTAINER_NAME}")
        container = database.create_container_if_not_exists(id=self.CONTAINER_NAME, partition_key=partition_key)

        return index_client, search_client, form_recognizer_client, cosmosdb_client, database, container


    def setup_crawler_data(self):
        # Retrieve the stored MD5 hash
        query = f"SELECT * FROM c"

        crawler_store_items = list(self.container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))

        return crawler_store_items

    def url_in_crawler_store(self, url, items):
        for item in items:
            if item['url'] == url:
                return True, item
        return False, None

    def base_crawler_consumer(self, q, nextq):
        self.logging.info(f"Base crawler consumer running.")
        while True:
            base_url = q.get()
            if base_url is None:
                break

            self.crawl_base_url(base_url, nextq)
            q.task_done()
        self.logging.info(f"Base Crawler Consumer is done")
            
    def crawl_base_url(self, base_url, nextq):

        try:
            with WebCrawler(base_url, exclude_urls=self.EXCLUDE_LIST) as crawler:
                self.logging.info(f"Crawling: {base_url}")
                crawler.visit_url(base_url)

                md5_hash = hashlib.md5(crawler.get_page_source().encode()).hexdigest()

                if not self.page_has_changed(url=base_url, md5_hash=md5_hash):
                    return

                table_dict = {}

                # Change in crawl logic 12/4/2023 to reflect feedback
                #table_dict = crawler.parse_tables()

                # if the page is not Opportunities page, then extract links and crawl the links. Assuming static content URL
                #if not table_dict:
                    #logging.info(f"No links found on: {base_url}, assuming static content url.")
                table_dict[base_url] = {}
                body = crawler.get_elements(By.TAG_NAME, "body")
                
                if len(body) > 0:
                    links = crawler.get_links(body[0], exclude=True, file_types=self.EXTRACT_LINK_TYPE)

                    table_dict[base_url]["links"] = links
                    table_dict[base_url]["metadata"] = {}
                else:
                    self.logging.info(f"No body found on: {base_url}.")

                # Add links to the next queue
                for key_link, table in table_dict.items():
                    try:
                        for link in table["links"]:
                            item = {"url": link, "metadata": table["metadata"]}
                            nextq.put(item)
                    except Exception as e:
                        self.logging.error(f"Error procesing base url links for : {key_link}, Error: {e}")
        except Exception as e:
            self.logging.error(f"Error processing base url: {base_url}, Error: {e}")


    def url_crawler_consumer(self, q, nextq):
        while True:
            item = q.get()
            if item is None:
                break

            result = self.crawl_url(item["url"])
            if result is not None:
                content, contenttype = result
                
                item["content"] = content
                item["contenttype"] = contenttype
                nextq.put(item)
            else:
                self.logging.warning(f"No data for url: {item['url']}")
                continue

            q.task_done()
        self.logging.info(f"Url Crawler Consumer is done")


    def crawl_url(self, url):
        """Crawl a URL and return its content and type."""
        self.logging.info(f"Crawling: {url}")
        try:
            if url.lower().endswith(".pdf"):
                response = requests.get(url)
                md5_hash = hashlib.md5(response.content).hexdigest()

                if not self.page_has_changed(url=url, md5_hash=md5_hash):
                    return None, None
                
                return response.content, "pdf"
            else:
                with WebCrawler(base_url=url, exclude_urls=self.EXCLUDE_LIST) as crawler:
                    crawler.visit_url(url)

                    md5_hash = hashlib.md5(crawler.get_page_source().encode()).hexdigest()

                    if not self.page_has_changed(url=url, md5_hash=md5_hash):
                        return None, None
                
                    content = crawler.parse_page()
                    return content, "text"
        except Exception as e:
            self.logging.error(f"Error retrieving url : {url}, Error: {e}")
            


    def chunker_consumer(self, q, nextq):
        while True:
            item = q.get()
            if item is None:
                break

            try:
                chunking_result = chunk_file_content(
                    item["content"],
                    file_format=item["contenttype"] if item["contenttype"] in ["pdf", "text"] else "text",
                    num_tokens=512,
                    min_chunk_size=10,
                    token_overlap=128,
                    url=item["url"],
                    add_embeddings=self.ENABLE_VECTORS,
                    form_recognizer_client=self.form_recognizer_client if item["contenttype"] == "pdf" else None,
                    use_layout=True if item["contenttype"] == "pdf" else False,
                    metadata = item.get("metadata", None)
                )

                i=0
                for chunk in chunking_result.chunks:
                    # Process each chunk
                    id = base64.urlsafe_b64encode((f"{item['url']}").encode("utf-8") ).decode("utf-8")
                    chunk.id = f"{id}-{i}"
                    chunk.sourcepage = str(i)
                    chunk.sourcefile = str(item["url"])

                    nextq.put(chunk)

                    i += 1

            except Exception as e:
                self.logging.error(f"Error processing item from chuncker queue: {e}")

            finally:
                q.task_done()
            
        self.logging.info(f"Chunker Consumer is done")


    def indexer_consumer(self, q, search_client, batch_size=100):
        
        self.logging.info(f"Indexing consumer running with batch size: {batch_size}")

        batch = []
        while True:
            item = q.get()
            if item is None:
                # If there are items left in the batch, upload them
                if batch:
                    try:
                        # Upload the documents to the index
                        upload_documents_to_index(docs=batch, search_client=search_client, upload_batch_size=len(batch))
                    except Exception as e:
                        self.logging.error(f"Error uploading document to index: {e}")
                break

            batch.append(item)

            # If the batch size is reached, upload the batch
            if len(batch) == batch_size:
                try:
                    # Upload the documents to the index
                    upload_documents_to_index(docs=batch, search_client=search_client, upload_batch_size=batch_size)
                except Exception as e:
                    self.logging.error(f"Error uploading document to index: {e}")
                finally:
                    # Clear the batch
                    batch = []

            q.task_done()

        self.logging.info(f"Indexer Consumer is done")

    def page_has_changed(self, url, md5_hash):
        
        url_hash = hashlib.md5(url.encode()).hexdigest()
        in_store, item = self.url_in_crawler_store(url, self.crawler_store_items)

        if in_store:
            stored_md5_hash = item['md5_hash']
        else:
            self.logging.info(f"Item does not exist in Crawler Store. {url}")
            # If the item does not exist, add it to Cosmos DB
            item = {'id': url_hash, 'url': url, 'md5_hash': md5_hash, 'not_seen_count' : 0}
            self.container.upsert_item(body=item)
            return True
            

        self.logging.info(f"For url: {url}  ---  md5_hash: {md5_hash}  ---  stored_md5_hash: {stored_md5_hash}")

        # Compare the computed MD5 hash with the stored hash
        if md5_hash != stored_md5_hash:
            self.logging.info(f"Page content has changed. {url}")
            # If the hash has changed, update the item in Cosmos DB
            #item = items[0]
            item['md5_hash'] = md5_hash
            item['not_seen_count'] = 0
            self.container.replace_item(item=item['id'], body=item)
            return True
        else:
            self.logging.info(f"Page content has not changed. {url}")
            return False

    def check_expired_links(self, crawler_store_items):
        
        self.logging.info(f"Checking expired links in crawler store.")

        expired_links = []
        for item in crawler_store_items:
            url = item['url']
            try:
                response = requests.head(url)
                if response.status_code == 404 or response.status_code == 403:

                    self.logging.info(f"Link unreachable, incrementing not seen count: {url}")

                    item['not_seen_count'] = item['not_seen_count'] + 1

                    if item['not_seen_count'] > 3:
                        self.logging.info(f"Link unreachable for 3 consecutive crawls, expiring url: {url}")
                        #container.delete_item(item=item['id'])
                        expired_links.append(url)
                    else:
                        self.container.replace_item(item=item['id'], body=item)

            except Exception as e:
                self.logging.error(f"Error occurred while checking {url}, Exception: {e}")
        return expired_links

    def delete_from_index(self, expired_links, search_client):

        self.logging.info(f"Deleteing expired links from index.")

        # Query the index for the ids of the expired links
        ids_to_delete = []
        for link in expired_links:
            results = search_client.search(search_text="*", select="id", filter=f"sourcefile eq '{link}'")
            for result in results:
                ids_to_delete.append(result["id"])

        # Delete the expired links from the index
        for id in ids_to_delete:
            self.logging.info(f"Deleteing from index: id = {id}")
            search_client.delete_documents(documents=[{"@search.action": "delete", "id": id}])

    def start_threads(self, consumer, source_queue, target_queue, num_of_threads):
        return [threading.Thread(target=consumer, args=(source_queue, target_queue)) for _ in range(num_of_threads)]


    def orchestrate(self):
    
        self.logging.info('Orchestrator is running...')

        create_search_index(index_name=self.INDEX_NAME, index_client=self.index_client)

        base_crawler_queue = queue.Queue()
        url_crawler_queue = queue.Queue()
        chunker_queue = queue.Queue()
        indexer_queue = queue.Queue()

        queues = [base_crawler_queue, url_crawler_queue, chunker_queue, indexer_queue]

        # Create multiple threads for each queue
        base_crawler_consumer_threads = self.start_threads(self.base_crawler_consumer, base_crawler_queue, url_crawler_queue, self.NUM_OF_THREADS)
        url_crawler_consumer_threads = self.start_threads(self.url_crawler_consumer, url_crawler_queue, chunker_queue, self.NUM_OF_THREADS)
        chunker_consumer_threads = self.start_threads(self.chunker_consumer, chunker_queue, indexer_queue, self.NUM_OF_THREADS)
        indexer_consumer_threads = [threading.Thread(target=self.indexer_consumer, args=(indexer_queue, self.search_client)) for _ in range(self.NUM_OF_THREADS)]

        all_threads = base_crawler_consumer_threads + url_crawler_consumer_threads + chunker_consumer_threads + indexer_consumer_threads

        # Start all the threads
        for thread in all_threads:
            thread.start()

        # For each base url add it to base url queue
        for base_url in self.BASE_URLS:
            base_crawler_queue.put(base_url)

        # For each base url add it to base url queue
        for url in self.CRAWL_URLS:
            item = dict()
            item["url"] = url
            url_crawler_queue.put(item=item)

        # Wait for all the queues to be processed
        for q in queues:
            q.join()

        # Signal the consumers to stop
        for _ in range(self.NUM_OF_THREADS):
            for q in queues:
                q.put(None)

        # Wait for all the threads to finish
        for thread in all_threads:
            thread.join()

        # Check for removed links
        expired_links = self.check_expired_links(self.crawler_store_items)
        
        # Remove links
        self.delete_from_index(expired_links, self.search_client)
