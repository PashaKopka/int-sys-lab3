import datetime
import json
import logging
from typing import List

import pydantic_core
import requests

from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_gateway import StoreGateway


class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url
    
    def _serialize_datetime(self, value):
        if isinstance(value, datetime.datetime):
            return value.isoformat()
        return value

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]):
        # Make a POST request to the Store API endpoint with the processed data
        data_to_send = [item.model_dump() for item in processed_agent_data_batch]
        response = requests.post(
            f"{self.api_base_url}/processed_agent_data",
            data=json.dumps(data_to_send, default=self._serialize_datetime),
        )
        if response.status_code != 200:
            logging.info(
                f"Failed to save data to the Store API. Status code: {response.status_code}"
            )
            return
