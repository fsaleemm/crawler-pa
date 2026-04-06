
from azure.search.documents.indexes.models import (
    SearchableField,
    SearchField,
    SimpleField,
    SearchFieldDataType,
    SemanticField,
    SemanticConfiguration,
    SearchIndex,
    VectorSearch,
    HnswAlgorithmConfiguration,
    VectorSearchProfile,
    SemanticSearch,
    SemanticPrioritizedFields,
    AzureOpenAIVectorizer,
    AzureOpenAIVectorizerParameters,
)

import dataclasses
import os
from tqdm import tqdm
import logging

def create_search_index(index_name, index_client, vectorizer_resource_uri, vectorizer_deployment_id, vectorizer_model_name):
    logging.info(f"Ensuring search index {index_name} exists")

    fields = [
        SimpleField(
            name="id",
            type=SearchFieldDataType.String,
            key=True,
            hidden=False,
            filterable=False,
            sortable=False,
            facetable=False,
            searchable=True,
            analyzer_name="keyword",
        ),
        SearchableField(
            name="content",
            type=SearchFieldDataType.String,
            hidden=False,
            filterable=False,
            sortable=False,
            facetable=False,
            analyzer_name="en.microsoft",
        ),
        SearchableField(
            name="title",
            type=SearchFieldDataType.String,
            hidden=False,
            filterable=False,
            sortable=False,
            facetable=False,
            analyzer_name="en.microsoft",
        ),
        SearchableField(
            name="filepath",
            type=SearchFieldDataType.String,
            hidden=False,
            filterable=False,
            sortable=False,
            facetable=False,
            analyzer_name="standard.lucene",
        ),
        SearchableField(
            name="url",
            type=SearchFieldDataType.String,
            hidden=False,
            filterable=False,
            sortable=False,
            facetable=False,
            analyzer_name="standard.lucene",
        ),
        SearchableField(
            name="metadata",
            type=SearchFieldDataType.String,
            hidden=False,
            filterable=False,
            sortable=False,
            facetable=False,
            analyzer_name="standard.lucene",
        ),
        SearchableField(
            name="extracted_data",
            type=SearchFieldDataType.String,
            hidden=False,
            filterable=False,
            sortable=False,
            facetable=False,
            analyzer_name="standard.lucene",
        ),
        SearchField(
            name="embedding",
            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
            searchable=True,
            hidden=False,
            vector_search_dimensions=1536,
            vector_search_profile_name="my-vector-profile",
        ),
        SimpleField(
            name="sourcepage",
            type=SearchFieldDataType.String,
            hidden=False,
            filterable=True,
            sortable=False,
            facetable=True,
        ),
        SimpleField(
            name="sourcefile",
            type=SearchFieldDataType.String,
            hidden=False,
            filterable=True,
            sortable=False,
            facetable=True,
        ),
    ]

    vector_search = VectorSearch(
        algorithms=[
            HnswAlgorithmConfiguration(name="default"),
        ],
        profiles=[
            VectorSearchProfile(
                name="my-vector-profile",
                algorithm_configuration_name="default",
                vectorizer_name="pgc-vectorizer",
            ),
        ],
        vectorizers=[
            AzureOpenAIVectorizer(
                vectorizer_name="pgc-vectorizer",
                parameters=AzureOpenAIVectorizerParameters(
                    resource_url=vectorizer_resource_uri,
                    deployment_name=vectorizer_deployment_id,
                    model_name=vectorizer_model_name,
                ),
            ),
        ],
    )

    semantic_search = SemanticSearch(
        configurations=[
            SemanticConfiguration(
                name="default",
                prioritized_fields=SemanticPrioritizedFields(
                    title_field=SemanticField(field_name="title"),
                    content_fields=[SemanticField(field_name="content")],
                ),
            ),
        ],
    )

    index = SearchIndex(
        name=index_name,
        fields=fields,
        vector_search=vector_search,
        semantic_search=semantic_search,
    )

    logging.info(f"Creating/updating {index_name} search index")
    result = index_client.create_or_update_index(index)
    logging.info(f"Index '{result.name}' created/updated successfully.")


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