


try:
    from kafka import KafkaConsumer
    import json
    import requests
    import os
    import sys
except Exception as e:
    pass

os.environ['KAFKA_TOPIC'] = "FirstTopic"
os.environ['SERVER_END_POINT'] = "http://localhost:9200"



def main():
    consumer = KafkaConsumer(os.getenv('KAFKA_TOPIC'))
    for msg in consumer:
    
        payload = json.loads(msg.value)
        payload["meta_data"]={
            "topic":msg.topic,
            "partition":msg.partition,
            "offset":msg.offset,
            "timestamp":msg.timestamp,
            "timestamp_type":msg.timestamp_type,
            "key":msg.key,
        }

    print(payload.get("meta_data").get("topic"))