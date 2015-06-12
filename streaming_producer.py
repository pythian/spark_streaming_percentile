import time
import json
import argparse

import streaming_generator
from kafka import SimpleProducer, KafkaClient


def timed_call(fn, calls_per_second, *args, **kwargs):
    start = time.time()
    fn(*args, **kwargs)
    fn_time = time.time() - start
    sleep_duration = abs((1.0 - calls_per_second * fn_time) / calls_per_second)
    print sleep_duration
    while True:
        fn(*args, **kwargs)
        time.sleep(sleep_duration)


def send_message(producer, topic):
    message_raw = streaming_generator.gen_random_message()
    producer.send_messages(topic, json.dumps({'user_id': message_raw[0],
                                              'activity': message_raw[1]}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--messages', default=1000)
    parser.add_argument('-z', '--host', default="127.0.0.1:9092")
    parser.add_argument('-t', '--topic', default='messages')
    args = parser.parse_args()
    kafka = KafkaClient(args.host)
    producer = SimpleProducer(kafka)
    timed_call(send_message, args.messages, producer, args.topic)
