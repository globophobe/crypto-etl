import gzip
import os
from tempfile import NamedTemporaryFile

import httpx
import pandas as pd


class HistoricalDownloader:
    def __init__(
        self,
        url,
        columns=("trdMatchID", "symbol", "timestamp", "size", "tickDirection", "price"),
    ):
        self.url = url
        self.columns = columns

    def main(self):
        # Streaming downloads with boto3, and httpx gave many EOFErrors.
        # No problem with regular download.
        response = httpx.get(self.url)
        if response.status_code == 200:
            temp_file = NamedTemporaryFile()
            filename = temp_file.name
            with open(filename, "wb+") as temp:
                temp.write(response.content)
                size = os.path.getsize(filename)
                if size > 0:
                    # Extract
                    return self._extract(filename)
                else:
                    print(f"No data: {self.url}")
        else:
            print(f"Error {response.status_code}: {self.url}")

    def _extract(self, filename):
        try:
            data_frame = pd.read_csv(
                filename,
                usecols=self.columns,
                engine="python",
                compression="gzip",
            )
        except EOFError:
            print(f"EOFError: {filename}")
            data_frame = self._extract_force(filename)
        return data_frame

    def _extract_force(self, filename):
        lines = []
        try:
            with gzip.open(filename, "rt") as f:
                for line in f:
                    lines.append(line)
        except EOFError:
            data = [line.strip().split(",") for line in lines]
            data_frame = pd.DataFrame(data[1:], columns=data[0])
        return data_frame
