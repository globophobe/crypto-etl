import os
import time

import google.auth
import pandas as pd
from google.auth.exceptions import TransportError
from google.cloud import bigquery

from ..constants import BIGQUERY_LOCATION, PROJECT_ID
from .lib import get_schema_columns


class BaseBigQueryLoader:
    def __init__(self, table_id, partition_decorator):
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self.bq = bigquery.Client(
            credentials=credentials,
            project=os.environ[PROJECT_ID],
            location=os.environ.get(BIGQUERY_LOCATION, None),
        )

        dataset, table_name = table_id.split(".")

        self.dataset = dataset
        self.table_id = table_id
        self.table_name = table_name
        self.partition_decorator = partition_decorator

    @property
    def partition(self):
        return f"{self.table_id}${self.partition_decorator}"

    def table_exists(self):
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("table", "STRING", self.table_name)
            ]
        )
        query = f"""
            SELECT size_bytes FROM {self.dataset}.__TABLES__
            WHERE table_id = @table;
        """
        job = self.bq.query(query, job_config=job_config)
        rows = job.result()
        return len(list(rows)) > 0

    def create_table(self, schema):
        table_id = f"{self.bq.project}.{self.dataset}.{self.table_name}"
        table = bigquery.Table(table_id, schema=schema)
        # Partition on date.
        table = self.set_partition(table)
        self.bq.create_table(table)

    def set_partition(self, table):
        raise NotImplementedError

    def read_table(self, sql, job_config, retry=5):
        # Retry n times
        r = retry - 1
        try:
            return self.read(sql, job_config)
        except TransportError as exception:
            if r == 0:
                raise exception
            else:
                time.sleep(5)
                self.read_table(sql, job_config, retry=r)

    def read(self, sql, job_config):
        query = self.bq.query(sql, job_config=job_config)
        return query.result().to_dataframe()

    def write_table(self, schema, data, retry=5):
        # Retry n times
        r = retry - 1
        try:
            self.write(schema, data)
        except TransportError as exception:
            if r == 0:
                raise exception
            else:
                time.sleep(5)
                self.write_table(schema, data, retry=r)

    def write(self, schema, data):
        if not self.table_exists():
            self.create_table(schema)
        job_config = bigquery.LoadJobConfig(
            schema=schema, write_disposition="WRITE_TRUNCATE"
        )
        if isinstance(data, pd.DataFrame):
            # If data_frame, get columns
            if len(data):
                columns = get_schema_columns(schema)
                data = data[columns]
                self.bq.load_table_from_dataframe(
                    data, self.partition, job_config=job_config
                ).result()
        else:
            # If json, assume correct
            self.bq.load_table_from_json(
                data, self.partition, job_config=job_config
            ).result()

    def delete_table(self):
        if self.table_exists():
            self.bq.delete_table(self.table_id)
