from kafka import KafkaConsumer
from typing import List


class StreamConsumer(object):
    def __init__(self, topics: List[str]):
        self.topics = topics
        self.consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092']
        )

    def start(self):
        self.consumer.subscribe(topics=self.topics)

        while True:
            poll_message = self.consumer.poll(timeout_ms=10)

            if poll_message is None or not poll_message.values():
                continue

            print(poll_message.va)
