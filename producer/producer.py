from kafka import KafkaProducer
from random import randint, uniform, choice
from datetime import datetime
import json
from dataclasses import dataclass
import time


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


# we generate timestamps in accelerated manner so our transactions lay in a period of hours, days
class FastForwardTime:
    def __init__(self, start_time: datetime, speedup_factor: float):
        self.start_time = start_time
        self.speedup_factor = speedup_factor
        self.actual_start_time = datetime.now()

    def get_current_time(self) -> datetime:
        elapsed_real_time = datetime.now() - self.actual_start_time
        elapsed_fast_forward_time = elapsed_real_time * self.speedup_factor
        current_fast_forward_time = self.start_time + elapsed_fast_forward_time
        return current_fast_forward_time


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
        latitude, longitude = generate_original_localization()
        cards[card_id] = {"user_id": user_id, "limit": limit, "latitude": latitude, "longitude": longitude}
    return cards


def generate_original_localization():
    latitude = uniform(-90, 90)
    longitude = uniform(-180, 180)
    return latitude, longitude


def generate_nearby_localization(latitude, longitude, max_offset=0.1):
    new_latitude = latitude + uniform(-max_offset, max_offset)
    new_longitude = longitude + uniform(-max_offset, max_offset)
    return new_latitude, new_longitude


def generate_transaction_value():
    float_value = uniform(1, 1001)
    return round(float_value, 2)


def probability_of_anomaly(counts_in_hour: int) -> float:
    probability_of_anomaly = 1 / (24 * 60 * 60 / LOOP_VIRTUAL_INTERVAL)
    return 1.0


def generate_transaction_above_limit_anomaly(cards, time: datetime) -> Transaction | None:
    # probability_of_anomaly = 1 / (24 * 60 * 60 / LOOP_VIRTUAL_INTERVAL)
    transaction = generate_transaction(cards, time)
    transaction.value = uniform(transaction.limit, transaction.limit + 100)
    return None


def generate_anomalies(transaction):
    anomaly_type = choice(['high_value'])  # , 'localization_change'
    if anomaly_type == 'high_value':
        transaction.value = uniform(transaction.limit, transaction.limit + 100)
    elif anomaly_type == 'localization_change':
        transaction.latitude = transaction.latitude + uniform(1,
                                                              50)  # teraz bierzemy tylko większe szerokości, ale (-1,1) może dać nam 0 czyli okej wartość, więc trzeba przemyśleć
        transaction.longitude = transaction.longitude + uniform(1, 50)
    # return


def time_str(str: str) -> datetime:
    return datetime.strptime(str, '%Y-%m-%d %H:%M:%S')


def generate_transaction(cards, time: datetime):
    card_id = choice(list(cards.keys()))
    card = cards[card_id]
    user_id = card['user_id']
    limit = card['limit']
    latitude, longitude = generate_nearby_localization(card['latitude'], card['longitude'])
    transaction_value = generate_transaction_value()
    timestamp = time
    return Transaction(
        card_id=card_id,
        timestamp=timestamp,
        value=transaction_value,
        user_id=user_id,
        limit=limit,
        latitude=latitude,
        longitude=longitude,
    )


SPEEDUP_FACTOR = 60 * 24  # one real minute is one virtual day
LOOP_VIRTUAL_INTERVAL = 15  # one main loop cycle every 15 virtual seconds
LOOP_REAL_INTERVAL = LOOP_VIRTUAL_INTERVAL / SPEEDUP_FACTOR  # one main loop cycle every LOOP_REAL_INTERVAL real seconds

if __name__ == '__main__':
    # producer = kafka_producer()
    topic = 'Transactions'
    start_datetime = time_str("2024-06-01 08:00:00")
    speedup_factor = 60 * 24  # now one real minute is one virtual day
    time_provider = FastForwardTime(start_time=start_datetime, speedup_factor=speedup_factor)
    cards = generate_cards()
    for i in range(1, 20):
        now = time_provider.get_current_time()
        transaction = generate_transaction(cards, time=now)
        anomalies = []
        if time_str("2024-06-01 08:00:00") <= now <= time_str("2024-06-01 10:00:00"):
            anomaly = generate_transaction_above_limit_anomaly(cards=cards, time=now)
            anomalies.append(anomaly)

        print(f'transaction: {transaction.json()}')
        # producer.send(topic, value=transaction.json(), timestamp_ms=now.timestamp())
        time.sleep(LOOP_REAL_INTERVAL)
    #
    # producer.flush()
    # producer.close()
