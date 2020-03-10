from kafka import KafkaProducer
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson
import requests
import time
from typing import Dict
import json


class StreamProducer(object):
    def __init__(self, feeds_and_topics: Dict[str, str]):
        self.feeds_and_topics = feeds_and_topics
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

    def start(self):
        previous_results = {}
        while True:
            for transit_feed_url, transit_feed_topic  in self.feeds_and_topics.items():
                print('getting feed {}'.format(transit_feed_url))
                feed = gtfs_realtime_pb2.FeedMessage()
                response = requests.get(transit_feed_url, allow_redirects=True)
                feed.ParseFromString(response.content)

                previous_result = previous_results.get(transit_feed_url)

                if previous_result is None or previous_result != feed.entity:
                    print('Poll resulted in producing {} messages'.format(len(feed.entity)))

                    for entity in feed.entity:
                        json_message = MessageToJson(entity)
                        self.producer.send(topic=transit_feed_topic, value=json_message)

                    previous_results[transit_feed_url] = feed.entity

                else:
                    print('Poll resulted in same results')

                self.producer.flush()

            time.sleep(10)
