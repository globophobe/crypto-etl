import datetime
import os
from unittest.mock import Mock

import firebase_admin
import google.auth
import main
import pytest
from cryptotick.constants import (
    BIGQUERY_LOCATION,
    BIGQUERY_TABLES,
    FIREBASE_ADMIN_CREDENTIALS,
    FIRESTORE_COLLECTIONS,
)
from cryptotick.providers.bitmex import BITMEX, XBTUSD
from cryptotick.providers.bitmex.constants import MIN_DATE
from cryptotick.utils import base64_encode_dict, get_env_list, set_environment
from firebase_admin import firestore
from firebase_admin.credentials import Certificate
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

mock_context = Mock()
mock_context.event_id = "1"
mock_context.timestamp = datetime.datetime.utcnow().isoformat()


def cleanup_bigquery():
    table_ids = get_env_list(BIGQUERY_TABLES)
    if table_ids:
        for table_id in table_ids:
            assert table_id.endswith("_test")
        credentials, project_id = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        bq = bigquery.Client(
            credentials=credentials,
            project=project_id,
            location=os.environ.get(BIGQUERY_LOCATION, None),
        )
        for table_id in table_ids:
            try:
                bq.delete_table(table_id)
            except NotFound:
                pass


def cleanup_firestore():
    collections = get_env_list(FIRESTORE_COLLECTIONS)
    if collections:
        for collection in collections:
            assert collection.endswith("-test")
        if "FIREBASE_INIT" not in os.environ:
            certificate = Certificate(os.environ[FIREBASE_ADMIN_CREDENTIALS])
            firebase_admin.initialize_app(certificate)
            os.environ["FIREBASE_INIT"] = "true"
        for collection in collections:
            docs = firestore.client().collection(collection).stream()
            for doc in docs:
                doc.reference.delete()


def cleanup():
    cleanup_bigquery()
    cleanup_firestore()


@pytest.fixture(autouse=True)
def setenv():
    # Setup
    set_environment()
    cleanup()
    yield True
    cleanup()


def test_bitmex_etl(capsys):
    date = MIN_DATE.isoformat()
    data = {"date": date, "symbols": XBTUSD}
    event = {"data": base64_encode_dict(data)}
    main.bitmex_perpetual(event, mock_context)
    out, err = capsys.readouterr()
    bitmex = BITMEX.capitalize()
    assert f"{bitmex} {XBTUSD}: {date} OK" in out
