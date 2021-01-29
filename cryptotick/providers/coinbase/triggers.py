import os
import time

from googleapiclient import discovery

from ...constants import BIGQUERY_LOCATION, PROJECT_ID
from ...utils import date_range, get_container_name
from .spot import CoinbaseSpotETL


class CoinbaseSpotETLAIPlatformTrigger(CoinbaseSpotETL):
    def main(self):
        has_data = all(
            [self.has_data(date) for date in date_range(self.date_from, self.date_to)]
        )
        if not has_data:
            project_id = os.environ[PROJECT_ID]
            region = os.environ[BIGQUERY_LOCATION]
            container_name = get_container_name()
            # Cache discovery false
            # https://github.com/googleapis/google-api-python-client/issues/299
            cloudml = discovery.build("ml", "v1", cache_discovery=False)
            training_inputs = {
                "scaleTier": "BASIC",
                "region": region,
                "masterConfig": {
                    "imageUri": container_name,
                },
                "scheduling": {"maxWaitTime": "7200s", "maxRunningTime": "3600s"},
                "args": [
                    "--script",
                    "coinbase_spot.py",
                    "--api-symbol",
                    self.api_symbol,
                    "--date-from",
                    self.date_from.isoformat(),
                    "--date-to",
                    self.date_to.isoformat(),
                    "--aggregate",
                ],
            }
            date = self.date_from.strftime("%Y%m%d")
            # No duplicate job ids
            t = round(time.time())
            job_id = f"{self.exchange}_{self.symbol}_{date}_{t}"
            job_spec = {"jobId": job_id, "trainingInput": training_inputs}
            cloudml.projects().jobs().create(
                body=job_spec, parent=f"projects/{project_id}"
            ).execute()
