import os

from ..constants import BIGQUERY_DATASET, BIGQUERY_TABLES
from ..utils import set_env_list


def get_table_id(exchange, suffix=""):
    if "PYTEST_CURRENT_TEST" in os.environ:
        suffix = f"{suffix}_test" if suffix else "test"
    table_name = f"{exchange}_{suffix}" if suffix else exchange
    dataset = os.environ[BIGQUERY_DATASET]
    table_id = f"{dataset}.{table_name}"
    set_env_list(BIGQUERY_TABLES, table_id)
    return table_id


def get_schema_columns(schema):
    return [field.name for field in schema]


def stringify_datetime_types(data):
    for key in ("date", "timestamp"):
        if key in data:
            data[key] = data[key].isoformat()
    return data
