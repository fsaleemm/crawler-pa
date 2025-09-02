from crawler.WebCrawler import WebCrawler
from chunker.text_chunker import chunk_file_content
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents import SearchClient
from indexer.search_manager import create_search_index
from indexer.search_manager import upload_documents_to_index
from azure.cosmos import CosmosClient, PartitionKey
from selenium.webdriver.common.by import By

import uuid, base64, os, time, requests, json
import queue, threading
import hashlib
from urllib.parse import urlparse
from azure.identity import DefaultAzureCredential
import re

class Orchestrator:
    _shared_state = {}

    def __init__(self, logger):
        self.__dict__ = self._shared_state

        regex_patterns = []

        if not self._shared_state:
            self.logging = logger
            self.DELAY = int(os.getenv("DELAY", 0))
            self.INDEXER_BATCH_SIZE = int(os.getenv("INDEXER_BATCH_SIZE", 100))
            self.CRAWL_DEPTH = int(os.getenv("DEPTH", 2))
            self.NUM_OF_THREADS = int(os.getenv("NUM_OF_THREADS", 1))
            self.EXCLUDE_LIST = os.getenv('EXCLUDE_LIST', "").split(',')
            self.EXCLUDE = False if self.EXCLUDE_LIST == [''] else True
            
            include_domains = os.getenv('INCLUDE_DOMAINS', "").split(',')
            self.INCLUDE_DOMAINS = False if include_domains == [''] else [include_domain.lower() for include_domain in include_domains]
            in_domain_regex_patterns = os.getenv('INCLUDE_DOMAINS_REGEX', '').split('|')
            in_domain_compiled_patterns = [re.compile(pattern) for pattern in in_domain_regex_patterns]
            self.INCLUDE_DOMAINS_REGEX = False if in_domain_regex_patterns == [''] else in_domain_compiled_patterns
            
            include_urls = os.getenv('INCLUDE_URLS', "").split(',')
            self.INCLUDE_URLS = False if include_urls == [''] else [include_urls.lower() for include_urls in include_urls]
            in_url_regex_patterns = os.getenv('INCLUDE_URLS_REGEX', '').split('|')
            in_url_compiled_patterns = [re.compile(pattern) for pattern in in_url_regex_patterns]
            self.INCLUDE_URLS_REGEX = False if in_url_regex_patterns == [''] else in_url_compiled_patterns
            
            self.BASE_URLS = os.getenv('BASE_URLS', "").split(',')
            extract_link_type = os.getenv('EXTRACT_LINK_TYPE', "").split(',')
            self.EXTRACT_LINK_TYPE = False if extract_link_type == [''] else [file_type.lower() for file_type in extract_link_type]
            self.CRAWL_URLS = os.getenv('CRAWL_URLS', "").split(',')

            enable_vectors_str = os.getenv("ENABLE_VECTORS", "false")
            self.ENABLE_VECTORS = enable_vectors_str.lower() in ['true', '1', 'yes']

            ignore_anchor_link = os.getenv("IGNORE_ANCHOR_LINK", "false")
            self.IGNORE_ANCHOR_LINK = ignore_anchor_link.lower() in ['true', '1', 'yes']

            self.INDEX_NAME = os.getenv("INDEX_NAME", "crawler-index")
            self.SEARCH_ENDPOINT = os.getenv("SEARCH_ENDPOINT")
            self.SEARCH_CREDS = AzureKeyCredential(os.getenv("SEARCH_KEY"))
            self.FORM_RECOGNIZER_ENDPOINT = os.getenv("FORM_RECOGNIZER_ENDPOINT") 
            self.FORM_RECOGNIZER_CREDS = AzureKeyCredential(os.getenv("FORM_RECOGNIZER_KEY"))
            self.COSMOS_URL = os.environ.get("COSMOS_URL")
            self.COSMOS_KEY = os.environ.get("COSMOS_DB_KEY", None)
            self.DATABASE_NAME = os.environ.get("COSMOS_DATABASE_NAME", "CrawlStore")
            self.CONTAINER_NAME = os.environ.get("COSMOS_CONTAINER_NAME", "URLChangeLog")
            self.AGENT_NAME = os.environ.get("AGENT_NAME", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0")

            self.index_client, self.search_client, self.form_recognizer_client, self.cosmosdb_client, self.database, self.container = self.setup_clients()
            self.crawler_store_items = self.setup_crawler_data()

        self.logging.info(f"NUM_OF_THREADS: {self.NUM_OF_THREADS}")
        self.logging.info(f"EXCLUDE_LIST: {self.EXCLUDE_LIST}")
        self.logging.info(f"EXCLUDE: {self.EXCLUDE}")
        self.logging.info(f"INCLUDE_DOMAINS: {self.INCLUDE_DOMAINS}")
        self.logging.info(f"INCLUDE_DOMAINS_REGEX: {in_domain_regex_patterns}")
        self.logging.info(f"INCLUDE_URLS: {self.INCLUDE_URLS}")
        self.logging.info(f"INCLUDE_URLS_REGEX: {in_url_regex_patterns}")
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
        
        cosmos_cred = self.COSMOS_KEY

        if not self.COSMOS_KEY:
            cosmos_cred = DefaultAzureCredential()
        
        cosmosdb_client = CosmosClient(self.COSMOS_URL, credential=cosmos_cred)
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



    def extract_links_to_queue(self, crawler, nextq, depth: int):
        self.logging.info(f"Extracting Links...")

        body = crawler.get_elements(By.TAG_NAME, "body")
        
        try:

            if len(body) > 0:
                links, p_links = crawler.get_links(body[0], exclude=self.EXCLUDE, file_types=self.EXTRACT_LINK_TYPE, include=(self.INCLUDE_URLS or self.INCLUDE_URLS_REGEX))
        
        
            #for p_link in p_links:
                #self.logging.info(f"Link found before applying rules : {p_link}")

            for link in links:
                self.logging.info(f"Link found, adding to crawler queue: {link} : at {depth}")
                item = {"url": link, "metadata": {}, "type": "base", "depth": depth + 1}

                if not self.item_in_queue(nextq, item):
                    nextq.put(item)
        except Exception as e:
            self.logging.error(f"Error in link extraction, Error: {e}")

    def item_in_queue(self, itemQueue, item):
        # Check if item with the same link already exists in the queue
        exists = False
        for q_item in list(itemQueue.queue):

            if q_item.get('url').lower() == item.get('url').lower():
                self.logging.info(f"Link already in crawler queue : {q_item.get('url')}")
                exists = True
                break
        
        return exists


    def url_crawler_consumer(self, q, nextq):
        while True:
            item = q.get()
            if item is None:
                break

            result = self.crawl_url(item["url"], q, item["type"], depth=item["depth"])
            if result is not None:
                content, contenttype = result

                item["content"] = content
                item["contenttype"] = contenttype
                nextq.put(item)

            q.task_done()

            if self.DELAY:
                self.logging.info(f"Pausing crawl for {self.DELAY} seconds.")
                time.sleep(self.DELAY)

        self.logging.info(f"Url Crawler Consumer is done")


    def crawl_url(self, url, q, url_type, depth):
        """Crawl a URL and return its content and type."""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.logging.info(f"Crawling: {url} of type: {url_type} and depth: {depth} (attempt {retry_count + 1}/{max_retries})")

                parsed_url = urlparse(url)
                
                if parsed_url.path.lower().endswith(".pdf"):
                    headers = {
                            "User-Agent": self.AGENT_NAME
                        }
                    response = requests.get(url=url, headers=headers, timeout=30)
                    
                    # Check if the request was successful
                    if response.status_code == 403:
                        self.logging.warning(f"Access forbidden (403) for PDF: {url}")
                        return None
                    elif response.status_code != 200:
                        self.logging.warning(f"HTTP {response.status_code} for PDF: {url}")
                        return None
                    
                    # Validate PDF content
                    if len(response.content) < 100:  # Minimum PDF size check
                        self.logging.warning(f"PDF content too small or empty: {url}")
                        return None
                    
                    # Check if content starts with PDF signature
                    if not response.content.startswith(b'%PDF'):
                        self.logging.warning(f"Invalid PDF format detected: {url}")
                        return None
                    
                    md5_hash = hashlib.md5(response.content).hexdigest()

                    if not self.page_has_changed(url=url, md5_hash=md5_hash):
                        return None
                    
                    return response.content, "pdf"
                else:
                    with WebCrawler(base_url=url, exclude_urls=self.EXCLUDE_LIST, agent=self.AGENT_NAME, include_domains=self.INCLUDE_DOMAINS, include_urls=self.INCLUDE_URLS, include_urls_regex=self.INCLUDE_URLS_REGEX, ignore_anchor_link=self.IGNORE_ANCHOR_LINK, include_domains_regex=self.INCLUDE_DOMAINS_REGEX) as crawler:
                        crawler.visit_url(url)
                        
                        #self.logging.info(f"URL : {url}, HTML: {crawler.get_page_source()}")

                        md5_hash = hashlib.md5(crawler.get_page_source().encode()).hexdigest()

                        if not self.page_has_changed(url=url, md5_hash=md5_hash):
                            return None

                        """ if url_type == "base":
                            depth = 0 """

                        if depth < self.CRAWL_DEPTH and url_type == "base":
                            self.extract_links_to_queue(crawler=crawler, nextq=q, depth=depth)

                        content = crawler.parse_page()
                        return content, "text"
                        
            except Exception as e:
                retry_count += 1
                error_msg = str(e).lower()
                
                # Check for specific error types that warrant retries
                if any(keyword in error_msg for keyword in ["invalid session id", "session", "timeout", "connection", "network"]):
                    self.logging.warning(f"Recoverable error for {url}, retry {retry_count}/{max_retries}: {e}")
                    if retry_count < max_retries:
                        time.sleep(2 * retry_count)  # Exponential backoff
                        continue
                else:
                    self.logging.error(f"Non-recoverable error retrieving url: {url}, Error: {e}")
                    break
        
        self.logging.error(f"Failed to crawl {url} after {max_retries} attempts")
        return None
            


    def chunker_consumer(self, q, nextq):
        while True:
            item = q.get()
            if item is None:
                break

            max_retries = 2
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    self.logging.info(f"Chunking consumer working on: {item['url']} (attempt {retry_count + 1}/{max_retries})")

                    # Validate content before processing
                    if item["contenttype"] == "pdf":
                        if not item["content"] or len(item["content"]) < 100:
                            self.logging.warning(f"Skipping PDF with insufficient content: {item['url']}")
                            break
                        
                        # Additional PDF validation
                        if not item["content"].startswith(b'%PDF'):
                            self.logging.warning(f"Skipping invalid PDF format: {item['url']}")
                            break

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
                        metadata = item.get("metadata", None),
                        logger=self.logging
                    )

                    i=0
                    for chunk in chunking_result.chunks:
                        # Process each chunk
                        id = base64.urlsafe_b64encode((f"{item['url']}").encode("utf-8") ).decode("utf-8")
                        chunk.id = f"{id}-{i}"
                        chunk.sourcepage = str(i)
                        chunk.sourcefile = str(item["url"])

                        if chunk.embedding is not None:
                            self.logging.info(f"Processed Chunk for url: {chunk.url} - Chunk id: {chunk.id} - Chunk embedding: {chunk.embedding[:5]}")
                        else:
                            self.logging.info(f"Processed Chunk for url: {chunk.url} - Chunk id: {chunk.id} - No embedding available")

                        nextq.put(chunk)

                        i += 1
                    
                    # If we get here, processing was successful
                    break
                    
                except Exception as e:
                    retry_count += 1
                    error_msg = str(e)
                    
                    # Check for specific Form Recognizer errors
                    if "InvalidContent" in error_msg or "corrupted" in error_msg.lower() or "unsupported" in error_msg.lower():
                        self.logging.warning(f"Form Recognizer content validation failed for {item['url']}: {e}")
                        if item["contenttype"] == "pdf":
                            # Try processing as text instead
                            self.logging.info(f"Attempting to process PDF as text for {item['url']}")
                            try:
                                text_content = f"Content from PDF: {item['url']}\n[PDF content could not be processed by Form Recognizer]"
                                
                                chunking_result = chunk_file_content(
                                    text_content,
                                    file_format="text",
                                    num_tokens=512,
                                    min_chunk_size=10,
                                    token_overlap=128,
                                    url=item["url"],
                                    add_embeddings=self.ENABLE_VECTORS,
                                    form_recognizer_client=None,
                                    use_layout=False,
                                    metadata = item.get("metadata", None),
                                    logger=self.logging
                                )
                                
                                # Process the fallback chunks
                                i=0
                                for chunk in chunking_result.chunks:
                                    id = base64.urlsafe_b64encode((f"{item['url']}").encode("utf-8") ).decode("utf-8")
                                    chunk.id = f"{id}-{i}"
                                    chunk.sourcepage = str(i)
                                    chunk.sourcefile = str(item["url"])
                                    nextq.put(chunk)
                                    i += 1
                                
                                self.logging.info(f"Successfully processed {item['url']} as fallback text")
                                break
                                
                            except Exception as fallback_error:
                                self.logging.error(f"Fallback processing also failed for {item['url']}: {fallback_error}")
                        break  # Don't retry for content validation errors
                    else:
                        self.logging.warning(f"Retryable error processing {item['url']}, retry {retry_count}/{max_retries}: {e}")
                        if retry_count < max_retries:
                            time.sleep(1 * retry_count)  # Brief delay before retry
                            continue
                        else:
                            self.logging.error(f"Failed to process {item['url']} after {max_retries} attempts: {e}")

            q.task_done()
            
        self.logging.info(f"Chunker Consumer is done")
            
        self.logging.info(f"Chunker Consumer is done")


    def indexer_consumer(self, q, search_client, batch_size=100):

        batch_size = self.INDEXER_BATCH_SIZE
        
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
                        self.logging.info(f"Search Indexer Consumer uploaded {len(batch)} documents to the index")
                    except Exception as e:
                        self.logging.error(f"Error uploading document to index: {e}")
                break

            batch.append(item)

            # If the batch size is reached, upload the batch
            if len(batch) == batch_size:
                try:
                    # Upload the documents to the index
                    upload_documents_to_index(docs=batch, search_client=search_client, upload_batch_size=batch_size)
                    self.logging.info(f"Search Indexer Consumer uploaded {len(batch)} documents to the index")
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

            if not url:
                continue

            try:
                parsed_url = urlparse(url)

                if self.INCLUDE_DOMAINS and parsed_url.netloc.lower() not in self.INCLUDE_DOMAINS:
                    self.logging.info(f"Expiring URL: {url} not matching include domains.")
                    self.container.delete_item(item=item['id'], partition_key=url)
                    expired_links.append(url)
                    continue


                response = requests.head(url)
                self.logging.info(f"URL: {url} -- RESPONSE: {response.status_code}")

                if response.status_code == 404 or response.status_code == 403:

                    self.logging.info(f"Link unreachable, incrementing not seen count: {url}")

                    item['not_seen_count'] = item['not_seen_count'] + 1

                    if item['not_seen_count'] > 3:
                        self.logging.info(f"Link unreachable for 3 consecutive crawls, expiring url: {url}")
                        self.container.delete_item(item=item['id'], partition_key=url)
                        expired_links.append(url)
                    else:
                        self.container.replace_item(item=item['id'], body=item)

                if self.DELAY:
                    self.logging.info(f"Pausing link check for {self.DELAY} seconds.")
                    time.sleep(self.DELAY)

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

        #base_crawler_queue = queue.Queue()
        url_crawler_queue = queue.Queue()
        chunker_queue = queue.Queue()
        indexer_queue = queue.Queue()

        #queues = [base_crawler_queue, url_crawler_queue, chunker_queue, indexer_queue]
        queues = [url_crawler_queue, chunker_queue, indexer_queue]

        # Create multiple threads for each queue
        #base_crawler_consumer_threads = self.start_threads(self.base_crawler_consumer, base_crawler_queue, url_crawler_queue, self.NUM_OF_THREADS)
        url_crawler_consumer_threads = self.start_threads(self.url_crawler_consumer, url_crawler_queue, chunker_queue, self.NUM_OF_THREADS)
        chunker_consumer_threads = self.start_threads(self.chunker_consumer, chunker_queue, indexer_queue, self.NUM_OF_THREADS)
        indexer_consumer_threads = [threading.Thread(target=self.indexer_consumer, args=(indexer_queue, self.search_client)) for _ in range(self.NUM_OF_THREADS)]

        #all_threads = base_crawler_consumer_threads + url_crawler_consumer_threads + chunker_consumer_threads + indexer_consumer_threads
        all_threads = url_crawler_consumer_threads + chunker_consumer_threads + indexer_consumer_threads

        # Start all the threads
        for thread in all_threads:
            thread.start()

        # For each base url add it to base url queue
        for base_url in self.BASE_URLS:
            
            if not base_url:
                continue

            item = dict()
            item["url"] = base_url
            item["type"] = "base"
            item["depth"] = 0

            url_crawler_queue.put(item=item)
            #base_crawler_queue.put(base_url)

        # For each base url add it to base url queue
        for url in self.CRAWL_URLS:

            if not url:
                continue

            item = dict()
            item["url"] = url
            item["type"] = "crawl"
            item["depth"] = 0

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
