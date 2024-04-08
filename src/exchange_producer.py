import json
from typing import List, Dict
from datetime import datetime, timezone
import time


from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
import requests

from config import EXCHANGES_URL, EXCHANGE_TOPIC, BOOTSTRAP_SERVERS
from schemas.exchange import Exchange

class ExchangeProducer(KafkaProducer):
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)

    @staticmethod
    def read_request(base_url) -> List[str]:
        "Get records from API as a List"
        records = []

        prefix = "https://"
        payload={}
        headers = {}

        utc_now = datetime.now().astimezone(timezone.utc) ##Add timestamp to when data occured TODO: Check API for this

        response = requests.request("GET", prefix+base_url, headers=headers, data=payload)
        data = response.json().get('data')
        print(data)
        for record in data:
            records.append(Exchange(arr=record, timestamp=utc_now))
        return records

    def publish_records(self, topic: str, messages: List):
        for records in messages:
            try:
                record = self.producer.send(topic=topic, key=records.exchangeId, value=records)
                print('Record {} successfully produced at offset {}'.format(records.exchangeId, record.get().offset))
            except KafkaTimeoutError as e:
                print(e.__str__())

if __name__ == '__main__':

    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'key_serializer': lambda key: str(key).encode(),
        'value_serializer': lambda x: json.dumps(x.__dict__, default=str).encode('utf-8')
    }
    producer = ExchangeProducer(props=config)
    asset = producer.read_request(base_url=EXCHANGES_URL)
    producer.publish_records(topic=EXCHANGE_TOPIC, messages=asset)