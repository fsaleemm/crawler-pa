
from azure.search.documents.indexes.models import (
    SearchableField,
    SearchField,
    SimpleField,
    SearchFieldDataType,
    SemanticField,
    SemanticSettings,
    SemanticConfiguration,
    SearchIndex,
    PrioritizedFields,
    VectorSearch,
    VectorSearchAlgorithmConfiguration,
    HnswParameters
)

import dataclasses
from tqdm import tqdm
import logging

def create_search_index(index_name, index_client):
    logging.info(f"Ensuring search index {index_name} exists")
    if index_name not in index_client.list_index_names():
        index = SearchIndex(
            name=index_name,
            fields=[
                SearchableField(name="id", type="Edm.String", key=True),
                SearchableField(
                    name="content", type="Edm.String", analyzer_name="en.lucene"
                ),
                SearchableField(
                    name="title", type="Edm.String", analyzer_name="en.lucene"
                ),
                SearchableField(name="filepath", type="Edm.String"),
                SearchableField(name="url", type="Edm.String"),
                SearchableField(name="metadata", type="Edm.String"),
                SearchableField(name="extracted_data", type="Edm.String"),
                SearchField(name="embedding", type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                            hidden=False, searchable=True, filterable=False, sortable=False, facetable=False,
                            vector_search_dimensions=1536, vector_search_configuration="default"),
                SimpleField(name="sourcepage", type="Edm.String", filterable=True, facetable=True),
                SimpleField(name="sourcefile", type="Edm.String", filterable=True, facetable=True),
            ],
            semantic_settings=SemanticSettings(
                configurations=[
                    SemanticConfiguration(
                        name="default",
                        prioritized_fields=PrioritizedFields(
                            title_field=SemanticField(field_name="title"),
                            prioritized_content_fields=[
                                SemanticField(field_name="content")
                            ],
                        ),
                    )
                ]
            ),
            vector_search=VectorSearch(
                algorithm_configurations=[
                    VectorSearchAlgorithmConfiguration(
                        name="default",
                        kind="hnsw",
                        hnsw_parameters=HnswParameters(metric="cosine")
                    )
                ]
            )
        )
        logging.info(f"Creating {index_name} search index")
        index_client.create_index(index)
    else:
        logging.info(f"Search index {index_name} already exists")


def upload_documents_to_index(docs, search_client, upload_batch_size=50):
    to_upload_dicts = []

    id = 0
    for document in docs:
        d = dataclasses.asdict(document)
        # add id to documents
        d.update({"@search.action": "upload"})
        if "embedding" in d and d["embedding"] is None:
            del d["embedding"]
        to_upload_dicts.append(d)
        id += 1

    # Upload the documents in batches of upload_batch_size
    #for i in tqdm(
    #    range(0, len(to_upload_dicts), upload_batch_size), desc="Indexing Chunks..."
    #):
    for i in range(0, len(to_upload_dicts), upload_batch_size):
        logging.info(f"Indexing Chunks ... {upload_batch_size}")
        batch = to_upload_dicts[i : i + upload_batch_size]
        results = search_client.upload_documents(documents=batch)
        num_failures = 0
        errors = set()
        for result in results:
            if not result.succeeded:
                logging.warning(
                    f"Indexing Failed for {result.key} with ERROR: {result.error_message}"
                )
                num_failures += 1
                errors.add(result.error_message)
        if num_failures > 0:
            raise Exception(
                f"INDEXING FAILED for {num_failures} documents. Please recreate the index."
                f"To Debug: PLEASE CHECK chunk_size and upload_batch_size. \n Error Messages: {list(errors)}"
            )
        

def upload_document_to_index(doc, search_client):

    d = dataclasses.asdict(doc)
    d.update({"@search.action": "mergeOrUpload"})
    if "embedding" in d and d["embedding"] is None:
        del d["embedding"]

    results = search_client .upload_documents(documents=d)
    num_failures = 0
    errors = set()
    for result in results:
        if not result.succeeded:
            logging.warning(
                f"Indexing Failed for {result.key} with ERROR: {result.error_message}"
            )
            num_failures += 1
            errors.add(result.error_message)
    if num_failures > 0:
        raise Exception(
            f"INDEXING FAILED for {num_failures} documents. Please recreate the index."
            f"To Debug: PLEASE CHECK chunk_size and upload_batch_size. \n Error Messages: {list(errors)}"
        )