import multiprocessing
from stream_consumer import StreamConsumer
from stream_producer import StreamProducer

streams_and_topics = {
        'http://gtfs.ovapi.nl/nl/tripUpdates.pb': 'trip_updates',
        'http://gtfs.ovapi.nl/nl/vehiclePositions.pb': 'vehicle_positions',
        'http://gtfs.ovapi.nl/nl/alerts.pb': 'alerts'
    }


def start_consumer():
    consumer = StreamConsumer(list(streams_and_topics.values()))
    consumer.start()


def start_producer():
    producer = StreamProducer(streams_and_topics)
    producer.start()


if __name__ == '__main__':
    consumer_process = multiprocessing.Process(name='steam_consumer', target=start_consumer)
    producer_process = multiprocessing.Process(name='stream_producer', target=start_producer)
    consumer_process.start()
    producer_process.start()
