import multiprocessing
from src.stream_producer import StreamProducer
from src.stream_consumer import StreamConsumer


def start_consumer():
    consumer = StreamConsumer(['trip_updates', 'vehicle_positions'])
    consumer.start()


def start_producer():
    producer = StreamProducer({
        'http://gtfs.ovapi.nl/nl/tripUpdates.pb': 'trip_updates',
        'http://gtfs.ovapi.nl/nl/vehiclePositions.pb': 'vehicle_positions'
    })
    producer.start()


if __name__ == '__main__':
    consumer_process = multiprocessing.Process(name='steam_consumer', target=start_consumer)
    producer_process = multiprocessing.Process(name='stream_producer', target=start_producer)
    consumer_process.start()
    producer_process.start()
