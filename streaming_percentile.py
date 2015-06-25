# coding: utf-8
import json
from operator import add

from tdigest import TDigest
from kafka import SimpleProducer, KafkaClient

from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
ssc = StreamingContext(sc, 3)
ssc.checkpoint('checkpoint')
kvs = KafkaUtils.createDirectStream(ssc, ["messages"],
                                    {"metadata.broker.list": "localhost:9092"})
scores = {'profile.picture.like': 2, 'profile.view': 1, 'message.private': 3}
scores_b = sc.broadcast(scores)


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


def publish_popular_users(percentile_value, popular_rdd):
    kafka = KafkaClient("localhost:9092")
    producer = SimpleProducer(kafka)
    producer.send_messages('popular_users',
                                   str(popular_rdd.map(lambda row: row[0]).collect()))
    producer.send_messages('popular_score',
                                   str(percentile_value))


def percentile_and_filter(rdd):
    digest = rdd.map(lambda row: row[1]).mapPartitions(
        digest_partitions).reduce(add)
    percentile_value = digest.percentile(0.95)
    filtered = rdd.filter(lambda row: row[1] > percentile_value)
    publish_popular_users(percentile_value, filtered)


def filter_most_popular(rdd):
    percentile_limit = rdd.map(lambda row: row[1]).mapPartitions(
        digest_partitions).reduce(add)
    percentile_limit_b = rdd.context.broadcast(percentile_limit)
    return rdd.filter(lambda row: row[1] > percentile_limit_b.value)

mapped = kvs.map(load_msg)
updated = mapped.updateStateByKey(update_scorecount)
updated.transform(filter_most_popular) # foreachRDD(percentile_and_filter)
updated.pprint()
ssc.start()
