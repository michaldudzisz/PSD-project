# Readme

## Prerequisites

Docker, Java 17 SDK, Maven, Python 3, Kafka-Python:

```bash
pip install kafka-python
```

## How to run

Start Kafka, Flink and Kafdrop with docker compose:

```bash
docker compose up
```

Wait few seconds for Kafka to start and start producer in another terminal:

```bash
python3 producer/producer.py
```
Build Flink executable and deploy it on Flink cluster:

```bash
sh ./build_and_deploy_flink.sh
```

## How to monitor

Enter `localhost:9000` in a web browser. You will see Kafdrop interface where you can monitor Kafka messages.
