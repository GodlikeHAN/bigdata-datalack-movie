from __future__ import annotations

from datetime import date, datetime

import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch, helpers

from src.config.settings import (
    ELASTICSEARCH_HOST,
    ELASTICSEARCH_PASSWORD,
    ELASTICSEARCH_USERNAME,
    ELASTICSEARCH_VERIFY_CERTS,
)
from src.indexing.elastic_mapping import INDEX_MAPPINGS


def get_elasticsearch_client() -> Elasticsearch:
    client_kwargs = {
        "hosts": ELASTICSEARCH_HOST,
        "verify_certs": ELASTICSEARCH_VERIFY_CERTS,
        "request_timeout": 60,
    }
    if ELASTICSEARCH_USERNAME:
        client_kwargs["basic_auth"] = (ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD)

    return Elasticsearch(
        **client_kwargs,
    )


def ensure_index(index_name: str) -> None:
    client = get_elasticsearch_client()
    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name, body=INDEX_MAPPINGS[index_name])


def _normalize_value(value):
    if value is None:
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, np.ndarray):
        return [_normalize_value(item) for item in value.tolist()]
    if isinstance(value, (list, tuple, set)):
        return [_normalize_value(item) for item in value]
    if isinstance(value, np.generic):
        value = value.item()
    if pd.isna(value):
        return None
    return value


def bulk_index_dataframe(index_name: str, dataframe: pd.DataFrame, id_column: str | None = None) -> int:
    ensure_index(index_name)
    client = get_elasticsearch_client()

    records = [
        {column: _normalize_value(value) for column, value in record.items()}
        for record in dataframe.to_dict(orient="records")
    ]
    actions = []
    for record in records:
        action = {"_index": index_name, "_source": record}
        if id_column and record.get(id_column) is not None:
            action["_id"] = str(record[id_column])
        actions.append(action)

    if not actions:
        return 0

    helpers.bulk(client, actions, refresh="wait_for")
    return len(actions)


def bulk_upsert_changed_dataframe(
    index_name: str,
    dataframe: pd.DataFrame,
    id_column: str,
    hash_column: str = "data_hash",
) -> dict[str, int]:
    ensure_index(index_name)
    client = get_elasticsearch_client()

    records = [
        {column: _normalize_value(value) for column, value in record.items()}
        for record in dataframe.to_dict(orient="records")
    ]
    records = [record for record in records if record.get(id_column)]

    if not records:
        return {"inserted": 0, "updated": 0, "skipped": 0}

    ids = [str(record[id_column]) for record in records]
    existing_docs = client.mget(index=index_name, ids=ids).get("docs", [])
    existing_hashes = {
        doc["_id"]: doc.get("_source", {}).get(hash_column)
        for doc in existing_docs
        if doc.get("found")
    }

    actions = []
    inserted = 0
    updated = 0
    skipped = 0

    for record in records:
        document_id = str(record[id_column])
        old_hash = existing_hashes.get(document_id)
        new_hash = record.get(hash_column)

        if old_hash == new_hash:
            skipped += 1
            continue

        if old_hash is None:
            inserted += 1
        else:
            updated += 1

        actions.append(
            {
                "_op_type": "index",
                "_index": index_name,
                "_id": document_id,
                "_source": record,
            }
        )

    if actions:
        helpers.bulk(client, actions, refresh="wait_for")

    return {"inserted": inserted, "updated": updated, "skipped": skipped}
