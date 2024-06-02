from kafka import KafkaProducer
from random import randint, uniform, choice
from datetime import datetime, timedelta
import json
from dataclasses import dataclass
import time
from math import radians, sin, cos, sqrt, atan2, degrees, asin


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
    def __init__(self, start_time: datetime):
        self.now = start_time

    def tick_by(self, secs: float):
        self.now = self.now + timedelta(seconds=secs)

    def get_current_time(self) -> datetime:
        return self.now


def kafka_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9094',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_cards():
    card_ids = list(range(1, 10_001))
    # 10 000 elements, at first from 1 to 7000, then from 1 to 3000:
    user_ids = iter(list(range(1, 7_001)) + list(range(1, 3_001)))
    cards = {}
    for card_id in card_ids:
        user_id = next(user_ids)
        limit = randint(500, 2001)
        latitude, longitude = generate_original_localization()
        cards[card_id] = {"user_id": user_id, "limit": limit, "latitude": latitude, "longitude": longitude}
    return cards


def generate_original_localization():
    latitude = uniform(-90, 90)
    longitude = uniform(-180, 180)
    return latitude, longitude


def generate_nearby_localization(latitude, longitude, max_distance_km = 15):
    earth_radius_km = 6371.0
    max_distance_rad = max_distance_km / earth_radius_km

    # Random distance with uniform distribution over the area
    random_distance_rad = sqrt(uniform(0, max_distance_rad ** 2))

    bearing = uniform(0, 360)
    bearing_rad = radians(bearing)

    lat_rad = radians(latitude)
    lon_rad = radians(longitude)

    new_lat_rad = asin(sin(lat_rad) * cos(random_distance_rad) +
                       cos(lat_rad) * sin(random_distance_rad) * cos(bearing_rad))
    new_lon_rad = lon_rad + atan2(sin(bearing_rad) * sin(random_distance_rad) * cos(lat_rad),
                                  cos(random_distance_rad) - sin(lat_rad) * sin(new_lat_rad))
    new_latitude = degrees(new_lat_rad)
    new_longitude = degrees(new_lon_rad)
    return new_latitude, new_longitude


def generate_transaction_value(limit: int):
    float_value = uniform(1, limit)
    return round(float_value, 2)


def probability_of_anomaly(counts_in_hour: float) -> float:
    number_of_function_call_in_hour = 60 * 60 / LOOP_VIRTUAL_INTERVAL
    probability = counts_in_hour / number_of_function_call_in_hour
    return probability


def generate_transaction_above_limit_anomaly(cards, time: datetime, user_id: int):
    probability = probability_of_anomaly(counts_in_hour=0.3)
    randed = uniform(0, 1)
    if randed < probability:
        transaction = generate_transaction(cards, user_id=user_id, time=time)
        limit = transaction.limit
        transaction.value = round(uniform(limit, 2 * limit), 2)
        return transaction
    else:
        return None


def generate_low_value_anomaly(cards, time, card_id: int):
    probability = probability_of_anomaly(counts_in_hour=0.5)
    randed = uniform(0, 1)
    if randed < probability:
        transaction = generate_transaction(cards, card_id=card_id, time=time)
        transaction.value = round(uniform(0, 0.5), 2)
        print(transaction.value)
        return transaction
    else:
        return None


def generate_anomaly_localization(latitude, longitude, min_distance_km=100):
    earth_radius_km = 6371.0
    min_distance_rad = min_distance_km / earth_radius_km
    bearing = uniform(0, 360)
    bearing_rad = radians(bearing)

    lat_rad = radians(latitude)
    lon_rad = radians(longitude)

    new_lat_rad = asin(sin(lat_rad) * cos(min_distance_rad) +
                       cos(lat_rad) * sin(min_distance_rad) * cos(bearing_rad))
    new_lon_rad = lon_rad + atan2(sin(bearing_rad) * sin(min_distance_rad) * cos(lat_rad),
                                  cos(min_distance_rad) - sin(lat_rad) * sin(new_lat_rad))

    new_latitude = degrees(new_lat_rad)
    new_longitude = degrees(new_lon_rad)
    return new_latitude, new_longitude


def generate_transaction_localization_change_anomaly(cards, time: datetime, user_id: int):
    randed = uniform(0, 1)
    probability = probability_of_anomaly(counts_in_hour=0.2)
    if randed < probability:
        transaction = generate_transaction(cards, time, user_id=user_id)
        new_latitude, new_longitude = generate_anomaly_localization(transaction.latitude, transaction.longitude)
        transaction.latitude = new_latitude
        transaction.longitude = new_longitude
        return transaction
    else:
        return None


# def generate_anomalies(transaction):
#     anomaly_type = choice(['high_value'])  # , 'localization_change'
#     if anomaly_type == 'high_value':
#         transaction.value = uniform(transaction.limit, transaction.limit + 100)
#     elif anomaly_type == 'localization_change':
#         transaction.latitude = transaction.latitude + uniform(1,
#                                                               50)  # teraz bierzemy tylko większe szerokości, ale (-1,1) może dać nam 0 czyli okej wartość, więc trzeba przemyśleć
#         transaction.longitude = transaction.longitude + uniform(1, 50)


def time_str(str: str) -> datetime:
    return datetime.strptime(str, '%Y-%m-%d %H:%M:%S')


def generate_transaction(cards, time: datetime, user_id: int = None, card_id: int = None):
    if user_id is not None and card_id is not None:
        raise RuntimeError("You should specify at most one of user id or card id")

    if user_id is not None:
        user_cards = []
        for card_id, card_info in cards.items():
            if card_info['user_id'] == user_id:
                user_cards.append(card_id)
        print("user_cards: " + str(user_cards))
        card_id = choice(user_cards)

    if card_id is not None:
        card_id = card_id

    if user_id is None and card_id is None:
        card_id = choice(list(cards.keys()))

    card = cards[card_id]
    user_id = card['user_id']
    limit = card['limit']
    latitude, longitude = generate_nearby_localization(card['latitude'], card['longitude'])
    transaction_value = generate_transaction_value(limit=limit)
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


LOOP_VIRTUAL_INTERVAL = 3  # one main loop cycle every 3 virtual seconds

if __name__ == '__main__':
    # producer = kafka_producer()
    # topic = 'Transactions'
    start_datetime = time_str("2024-06-01 08:00:00")
    time_provider = FastForwardTime(start_time=start_datetime)
    # users = generate_users()
    cards = generate_cards()
    now = time_provider.get_current_time()
    while now < time_str("2024-06-06 16:00:00"):
        now = time_provider.get_current_time()
        transaction = generate_transaction(cards, time=now)
        anomalies = []
        if time_str("2024-06-04 12:00:00") <= now <= time_str("2024-06-06 12:00:00"):
            anomaly = generate_transaction_above_limit_anomaly(cards=cards, time=now, user_id=100)
            if anomaly:
                print(f'Above limit anomaly generated: {anomaly.json()}')
            anomalies.append(anomaly)
        if time_str("2024-06-04 12:00:00") <= now <= time_str("2024-06-06 12:00:00"):
            anomaly = generate_low_value_anomaly(cards=cards, time=now, card_id=101)
            anomalies.append(anomaly)
        if time_str("2024-06-04 12:00:00") <= now <= time_str("2024-06-06 12:00:00"):
            anomaly = generate_transaction_localization_change_anomaly(cards=cards, time=now, user_id=102)
            if anomaly:
                print(f'Localization change anomaly generated: {anomaly.json()}')
            anomalies.append(anomaly)
        anomalies = list(filter(lambda anomaly: anomaly is not None, anomalies))
        transactions = [transaction] + anomalies
        # for transaction in transactions:
        #     print(f'transaction: {transaction.json()}')
            # producer.send(topic, value=transaction.json(), timestamp_ms=int(now.timestamp()))
        time_provider.tick_by(LOOP_VIRTUAL_INTERVAL)

    # producer.flush()
    # producer.close()

