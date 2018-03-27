#!/usr/bin/env python3

import json
import logging
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from telegraf import router as telegraf_router, counters, flush_stats

log = logging.getLogger(__name__)
LOG_FORMAT = '%(asctime)-15s %(levelname)s %(processName)s:%(threadName)s->%(name)s: %(message)s'

FLUSH_INTERVAL = 8
MAX_BUFFER_LEN = 5000


def message_handler(message, router, kafka_producer):
    if len(message.value()) == 0:
        return True
    try:
        message = json.loads(message.value().decode('utf-8'))
    except ValueError:
        log.debug('Error parsing message: {}'.format(message.value()))
        return True
    return router(message, kafka_producer)


def main_loop(kafka_consumer, kafka_producer, router):
    running = True
    log.info('Starting main loop...')
    last_flush = time.time()
    previous_lag = {}
    while running:
        now = time.time()
        if now - last_flush >= FLUSH_INTERVAL:
            log.debug('After {:.02f} seconds, flushed {} points to postgres with {} errors'.format(now - last_flush, counters['inserts'], counters['errors']))
            kafka_consumer.commit()
            kafka_producer.flush()
            total_lag = 0
            my_positions = kafka_consumer.position(kafka_consumer.assignment())
            for part in my_positions:
                try:
                    low, high = kafka_consumer.get_watermark_offsets(part, cached=False)
                except KafkaException as e:
                    log.error('{}'.format(e.args[0].str()))
                    continue
                if part.partition not in previous_lag:
                    previous_lag[part.partition] = 0
                if part.offset > 0:
                    lag = high - part.offset
                    log.debug(' - partition {} lag {} ({})'.format(part.partition, lag, lag - previous_lag[part.partition]))
                    total_lag += lag
                    previous_lag[part.partition] = lag
            counters['total_lag'] = total_lag
            flush_stats(now - last_flush)
            last_flush = now
        try:
            msg = kafka_consumer.poll(timeout=0.2)
            if msg is None:
                continue
            elif msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                log.error(msg.error())
                continue
            else:
                while not message_handler(msg, router, kafka_producer):
                    time.sleep(1)

        except KeyboardInterrupt:
            log.info('CTRL-C detected, exiting...')
            return
        except Exception:
            log.exception('Exception in main loop')


def assign_cb(consumer_, partitions):
    log.info("Partitions reassigned: ")
    for part in partitions:
        log.info("  - {}".format(part.partition))


def main_process(brokers, topic_name: str, message_router):
    conf = {
        'bootstrap.servers': brokers,
        'group.id': 'kafkapost',
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'auto.commit.enable': False,
        'api.version.request': True,
        'partition.assignment.strategy': 'roundrobin',
        'compression.codec': 'snappy'
    }
    c = Consumer(conf)
    c.subscribe([topic_name], on_assign=assign_cb)
    log.info('Subscribed to Kafka topic')

    p = Producer(conf)

    main_loop(c, p, message_router)
    log.debug('Unsubscribing from topic...')
    c.unsubscribe()
    log.debug('Closing Kafka consumer....')
    c.close()


def main():
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    main_process('bf11:9092,bf5:9092', 'telegraf_json', telegraf_router)


if __name__ == "__main__":
    main()
