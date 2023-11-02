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
import uuid, base64, os, time, logging, requests
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

# Constants
NUM_OF_THREADS = int(os.getenv("NUM_OF_THREADS"))
EXCLUDE_LIST = os.getenv('EXCLUDE_LIST').split(',')
BASE_URLS = os.getenv('BASE_URLS').split(',')


# Azure Search
INDEX_NAME = os.getenv("INDEX_NAME")
SEARCH_ENDPOINT = os.getenv("SEARCH_ENDPOINT")
SEARCH_CREDS = AzureKeyCredential(os.getenv("SEARCH_KEY"))

index_client = SearchIndexClient(endpoint=SEARCH_ENDPOINT, credential=SEARCH_CREDS)
search_client = SearchClient(
    endpoint=SEARCH_ENDPOINT, credential=SEARCH_CREDS, index_name=INDEX_NAME
)

create_search_index(index_name=INDEX_NAME, index_client=index_client)

# Azure Form Recognizer
FORM_RECOGNIZER_ENDPOINT = os.getenv("FORM_RECOGNIZER_ENDPOINT") 
FORM_RECOGNIZER_CREDS = AzureKeyCredential(os.getenv("FORM_RECOGNIZER_KEY"))

form_recognizer_client = DocumentAnalysisClient(
    endpoint=FORM_RECOGNIZER_ENDPOINT,
    credential=FORM_RECOGNIZER_CREDS,
)

def base_crawler_consumer(q, nextq):
    """Consumer function to process base URLs from the queue and add their links to the next queue."""
    while True:
        base_url = q.get()
        if base_url is None:
            break

        print(f"Crawling: {base_url}")
        crawl_base_url(base_url, nextq)
        q.task_done()
    print(f"Base Crawler Consumer is done")
        
def crawl_base_url(base_url, nextq):
    """Crawl a base URL and add its links to the next queue."""
    crawler = WebCrawler(base_url=base_url, exclude_urls=EXCLUDE_LIST)

    try:
        crawler.visit_url(base_url)
        table_dict = crawler.parse_tables()

        # Add links to the next queue
        for key_link, table in table_dict.items():
            try:
                for link in table["links"]:
                    item = {"url": link, "metadata": table["metadata"]}
                    nextq.put(item)
            except Exception as e:
                print(f"Error processing url: {key_link}, Error: {e}")
    except Exception as e:
        print(f"Error processing base url: {base_url}, Error: {e}")
    finally:
        crawler.close()


def url_crawler_consumer(q, nextq):
    """Consumer function to process URLs from the queue and add them to the next queue."""
    while True:
        item = q.get()
        if item is None:
            break

        print(f"Crawling: {item['url']}")
        result = crawl_url(item["url"])
        if result is not None:
            content, contenttype = result
            item["content"] = content
            item["contenttype"] = contenttype
            nextq.put(item)
        else:
            print(f"No data for url: {item['url']}")
            continue

        q.task_done()
    print(f"Url Crawler Consumer is done")


def crawl_url(url):
    """Crawl a URL and return its content and type."""

    try:
        if url.lower().endswith(".pdf"):
            response = requests.get(url)
            return response.content, "pdf"
        else:
            crawler = WebCrawler(base_url=url, exclude_urls=EXCLUDE_LIST)
            crawler.visit_url(url)
            content = crawler.parse_page()
            crawler.close()
            return content, "text"
    except Exception as e:
        print(f"Error processing url: {url}, Error: {e}")
        


def chunker_consumer(q, nextq):
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
                add_embeddings=True,
                form_recognizer_client=form_recognizer_client if item["contenttype"] == "pdf" else None,
                use_layout=True if item["contenttype"] == "pdf" else False,
                metadata=item["metadata"]
            )

            i=0
            for chunk in chunking_result.chunks:
                # Process each chunk
                id = base64.b64encode( (f"{item['url']}").encode("utf-8") ).decode("utf-8")
                chunk.id = f"{id}-{i}"
                chunk.sourcepage = str(i)
                chunk.sourcefile = str(item["url"])

                nextq.put(chunk)

                i += 1

        except Exception as e:
            print(f"Error processing item from queue: {e}")

        finally:
            q.task_done()
        
    print(f"Chunker Consumer is done")


def indexer_consumer(q, search_client, batch_size=100):
    """Consumer function to process items from the queue and upload them to the index."""
    batch = []
    while True:
        item = q.get()
        if item is None:
            # If there are items left in the batch, upload them
            if batch:
                try:
                    # Upload the documents to the index
                    upload_documents_to_index(docs=batch, search_client=search_client)
                    #pass
                except Exception as e:
                    print(f"Error uploading document to index: {e}")
            break

        batch.append(item)

        # If the batch size is reached, upload the batch
        if len(batch) == batch_size:
            try:
                # Upload the documents to the index
                upload_documents_to_index(docs=batch, search_client=search_client, upload_batch_size=batch_size)
                #pass
            except Exception as e:
                print(f"Error uploading document to index: {e}")
            finally:
                # Clear the batch
                batch = []

        q.task_done()

    print(f"Indexer Consumer is done")

def start_threads(consumer, source_queue, target_queue, num_of_threads):
    return [threading.Thread(target=consumer, args=(source_queue, target_queue)) for _ in range(num_of_threads)]


def main():
    start_time = time.time()
    formatted_start_time = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Starting crawler: {formatted_start_time}")

    base_crawler_queue = queue.Queue()
    url_crawler_queue = queue.Queue()
    chunker_queue = queue.Queue()
    indexer_queue = queue.Queue()

    queues = [base_crawler_queue, url_crawler_queue, chunker_queue, indexer_queue]

    # Create multiple threads for each queue
    base_crawler_consumer_threads = start_threads(base_crawler_consumer, base_crawler_queue, url_crawler_queue, NUM_OF_THREADS)
    url_crawler_consumer_threads = start_threads(url_crawler_consumer, url_crawler_queue, chunker_queue, NUM_OF_THREADS)
    chunker_consumer_threads = start_threads(chunker_consumer, chunker_queue, indexer_queue, NUM_OF_THREADS)
    indexer_consumer_threads = [threading.Thread(target=indexer_consumer, args=(indexer_queue, search_client)) for _ in range(NUM_OF_THREADS)]

    all_threads = base_crawler_consumer_threads + url_crawler_consumer_threads + chunker_consumer_threads + indexer_consumer_threads

    # Start all the threads
    for thread in all_threads:
        thread.start()

    # For each base url add it to base url queue
    for base_url in BASE_URLS:
        base_crawler_queue.put(base_url)

    # Wait for all the queues to be processed
    for q in queues:
        q.join()

    # Signal the consumers to stop
    for _ in range(NUM_OF_THREADS):
        for q in queues:
            q.put(None)

     # Wait for all the threads to finish
    for thread in all_threads:
        thread.join()


    end_time = time.time()
    formatted_end_time = datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Ending crawler: {formatted_end_time}")

    elapsed_time = end_time - start_time
    print("Elapsed time: {:.2f} seconds".format(elapsed_time))

if __name__ == "__main__":
    main()