from kafka import KafkaProducer
from random import randint, uniform, choice
from datetime import datetime
import json
import time
import numpy as np
from dataclasses import dataclass


@dataclass
class Transaction:
    card_id: str
    timestamp: datetime
    value: float
    user_id: str
    limit: int
    latitude: float
    longitude: float

    def json(self):
        return {
        'cardId': str(self.card_id),
        'timestamp': self.timestamp.isoformat(),
        'value': str(self.value),
        'userId': str(self.user_id),
        'limit': str(self.limit),
        'localization': {
            'latitude': self.latitude,
            'longitude': self.longitude,
        },
    }
    

def kafka_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9094',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_cards():
    card_ids = list(range(0, 10001))
    user_ids = list(range(0, 7001))
    cards = {}
    for card_id in card_ids:
        user_id = choice(user_ids)
        limit = randint(500, 2001)
        cards[card_id] = {"user_id" : user_id, "limit" : limit}
    return cards

def generate_localization():
    latitude = uniform(-90, 90)
    longitude = uniform(-180, 180)
    return latitude, longitude

def generate_transaction_value():
    return uniform(1, 1001)

def generate_anomalies(transaction):
    anomaly_type = choice(['high_value'])
    if anomaly_type == 'high_value':
        transaction.value = uniform(transaction.limit,  transaction.limit+100)
    # return

def generate_transaction(cards):
    card_id = choice(list(cards.keys()))
    card = cards[card_id]
    user_id = card['user_id']
    limit = card['limit']
    latitude, longitude = generate_localization()
    transaction_value = generate_transaction_value()
    timestamp = datetime.now()
    return Transaction(
        card_id=card_id,
        timestamp=timestamp,
        value=transaction_value,
        user_id=user_id,
        limit=limit,
        latitude=latitude,
        longitude=longitude,
    )

if __name__ == '__main__':
    producer = kafka_producer()
    topic = 'Transactins'

    cards = generate_cards()
    for i in range(1, 10):
        transaction = generate_transaction(cards)
        if True: # randint(1, 10) > 8:
            generate_anomalies(transaction)
        print(f'transaction: {transaction.json()}')
        producer.send(topic, value=transaction)
        time.sleep(1)

    producer.flush()
    producer.close()
