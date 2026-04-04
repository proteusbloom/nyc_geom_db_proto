from sodapy import Socrata
import os

class SocrataClient:
    def __init__(self):
        self.domain = 'data.cityofnewyork.us'
        self.app_token = os.getenv('SOCRATA_APP_TOKEN')
        self.client = Socrata(
            self.domain,
            app_token=self.app_token,
            timeout=100
        self.table_name = {}
        )

    def get_records(self, table, filters=None):
        response = self.session.get(f"{self.base_url}/{table}", params=filters)
        response.raise_for_status()
        return response.json()

    def run_query(self, query):
        response = self.session.post(f"{self.base_url}/query", json={"sql": query})
        response.raise_for_status()
        return response.json()

client = DBApiClient(base_url="https://...", api_key="...")
data = client.get_records("users", filters={"active": True})