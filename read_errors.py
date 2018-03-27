#!/usr/bin/env python3

import json
from pprint import pprint
import sys

from confluent_kafka import Consumer, KafkaError, KafkaException, OFFSET_END


def message_handler(message):
    if len(message.value()) == 0:
        return True
    try:
        message = json.loads(message.value().decode('utf-8'))
    except ValueError:
        print('Error parsing message: {}'.format(message.value()))


    pprint(message)


def main_loop(kafka_consumer):
    running = True
    print('Starting main loop...')
    previous_lag = {}
    while running:
        kafka_consumer.commit()
        total_lag = 0
        my_positions = kafka_consumer.position(kafka_consumer.assignment())
        for part in my_positions:
            try:
                low, high = kafka_consumer.get_watermark_offsets(part, cached=False)
            except KafkaException as e:
                print('{}'.format(e.args[0].str()))
                continue
            if part.partition not in previous_lag:
                previous_lag[part.partition] = 0
            if part.offset > 0:
                lag = high - part.offset
                total_lag += lag
                previous_lag[part.partition] = lag
        print('--> {} error messages to go...'.format(total_lag))
        try:
            msg = kafka_consumer.poll(timeout=0.2)
            if msg is None:
                continue
            elif msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                continue
            else:
                message_handler(msg)

        except KeyboardInterrupt:
            print('CTRL-C detected, exiting...')
            return
        except Exception:
            print('Exception in main loop')
        print('ENTER to continue...')
        sys.stdin.readline()


def assign_cb(consumer, partitions):
    print("Partitions reassigned: ")
    for part in partitions:
        print("  - {}".format(part.partition))
        part.offset = OFFSET_END
    consumer.commit()


def main_process(brokers, topic_name: str):
    conf = {
        'bootstrap.servers': brokers,
        'group.id': 'kafkapost',
        'default.topic.config': {'auto.offset.reset': 'latest'},
        'auto.commit.enable': True,
        'api.version.request': True,
        'partition.assignment.strategy': 'roundrobin',
        'compression.codec': 'snappy'
    }
    c = Consumer(conf)
    c.subscribe([topic_name], on_assign=assign_cb)
    print('Subscribed to Kafka topic')

    main_loop(c)
    print('Unsubscribing from topic...')
    c.unsubscribe()
    print('Closing Kafka consumer....')
    c.close()


def main():
    main_process('bf11:9092,bf5:9092', 'metrics_errors')


if __name__ == "__main__":
    main()
