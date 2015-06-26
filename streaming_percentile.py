# coding: utf-8
import json
from time import time
from operator import add
import argparse

from tdigest import TDigest
from kafka import KeyedProducer, RoundRobinPartitioner, KafkaClient

from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark import SparkContext


percentile_broadcast = None


def load_msg(msg):
    message = json.loads(msg[1])
    return message['user_id'], scores_b.value[message['activity']]


def update_scorecount(new_scores, score_sum):
    if not score_sum:
        score_sum = 0
    return sum(new_scores) + score_sum


def digest_partitions(values):
    digest = TDigest()
    digest.batch_update(values)
    return [digest]


def publish_popular_users(popular_rdd):
    key = 'popular_{}'.format(int(time()))
    message_key = popular_rdd.context.broadcast(key)

    def publish_partition(partition):
        kafka = KafkaClient(args_broadcast.value.kafka_hosts)
        producer = KeyedProducer(kafka, partitioner=RoundRobinPartitioner,
                                 async=True, batch_send=True)
        producer.send_messages('popular_users', message_key.value,
                               *[json.dumps(user) for user in partition])
    popular_rdd.foreachPartition(publish_partition)


def compute_percentile(rdd):
    global percentile_broadcast
    percentile_limit = rdd.map(lambda row: row[1]).mapPartitions(
        digest_partitions).reduce(add).percentile(args_broadcast.value.limit)
    percentile_broadcast = rdd.context.broadcast(
        percentile_limit)


def filter_most_popular(rdd):
    global percentile_broadcast
    if percentile_broadcast:
        return rdd.filter(lambda row: row[1] > percentile_broadcast.value)
    return rdd.context.parallelize([])


if __name__ == '__main__':
    percentile_braodcast = None
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', '--kafka-hosts', nargs='+',
                        default="localhost:9092")
    parser.add_argument(
        '-l', '--limit', default=0.95,
        help='Percentile above ehich people are considered popular.')
    parser.add_argument('-r', '--rate', default=3, help='size of micro batch')
    args = parser.parse_args()

    sc = SparkContext(appName="streaming_percentile")
    ssc = StreamingContext(sc, args.rate)
    ssc.checkpoint('checkpoint')
    global args_broadcast
    args_broadcast = sc.broadcast(args)

    kvs = KafkaUtils.createDirectStream(ssc, ["messages"],
                                        {"metadata.broker.list": args.kafka_hosts})
    scores = {'profile.picture.like': 2, 'profile.view': 1, 'message.private': 3}
    scores_b = sc.broadcast(scores)
    mapped = kvs.map(load_msg)
    updated = mapped.updateStateByKey(update_scorecount)
    updated.foreachRDD(compute_percentile)
    popular_stream = updated.transform(filter_most_popular)
    popular_stream.foreachRDD(publish_popular_users)
    ssc.start()
    ssc.awaitTermination()
