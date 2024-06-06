from kafka import KafkaConsumer
import json
import pandas as pd


def prepare_file(csv_file):
    header = "transaction.cardId,transaction.timestamp,transaction.value,transaction.userId,transaction.limit,transaction.localization.latitude,transaction.localization.longitude,fraudReason"
    with open(csv_file, 'w') as f:
        f.write(header + '\n')


def process_and_save_to_csv(messages, csv_file):
    df = pd.DataFrame(messages)
    df.to_csv(csv_file, index=False, header=False, mode='a')


def flatten_json(json_object):
    def _flatten(obj, prefix=''):
        items = {}
        for key, value in obj.items():
            new_key = f"{prefix}{key}"
            if isinstance(value, dict):
                items.update(_flatten(value, new_key + '.'))
            else:
                items[new_key] = value
        return items

    return _flatten(json_object)


def custom_deserializer(x):
    json_object = json.loads(x.decode('utf-8'))
    flattened_object = flatten_json(json_object)
    return flattened_object


consumer = KafkaConsumer(
    'Frauds',
    bootstrap_servers='localhost:9094',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='psd-group',
    value_deserializer=custom_deserializer
)

if __name__ == '__main__':
    csv_file = 'frauds.csv'
    prepare_file(csv_file)
    messages = []
    print("Listening to Kafka topic 'Frauds'...")
    try:
        for message in consumer:
            messages.append(message.value)
            if len(messages) >= 10_000:
                process_and_save_to_csv(messages, csv_file)
                messages = []
                print("appending to file...")
    except KeyboardInterrupt:
        print("Stopping listening")

    if messages:
        process_and_save_to_csv(messages, csv_file)

    print(f"Messages stored in file {csv_file}")
