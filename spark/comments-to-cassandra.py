from __future__ import print_function
import sys
import json
import copy
import time
import pprint
import datetime
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import BatchStatement
from cassandra.util import uuid_from_time
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from os import path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from config import ES_HOST, CASSANDRA_HOST, SPARK_MASTER, KAFKA_BROKER_1, KAFKA_BROKER_2, KAFKA_BROKER_3


def writeToCassandra(comment):
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect('reddit_comments')
        prepared_insert = session.prepare("""
            INSERT INTO comments (
                inserted_time, id, word, created_utc_uuid,
                created_utc, link_title, body, author,
                subreddit, parent_id, over_18, ups,
                downs, controversiality, gilded, score)
            VALUES (toUnixTimestamp(now()),?,?,?,
                ?,?,?,?,
                ?,?,?,?,
                ?,?,?,?)
        """)
        prepared_insert.consistency_level = ConsistencyLevel.ANY
        batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)
        batch.add(prepared_insert, (
            comment['id'],
            comment['word'],
            uuid_from_time(datetime.datetime.fromtimestamp(comment['created_utc'])),
            datetime.datetime.fromtimestamp(comment['created_utc']),
            comment['link_title'],
            comment['body'],
            comment['author'],
            comment['subreddit'],
            comment['parent_id'],
            comment['over_18'],
            comment['ups'],
            comment['downs'],
            comment['controversiality'],
            comment['gilded'],
            comment['score']
        ))
        session.execute(batch)
    except NoHostAvailable:
        print('lliilloic')
    return comment


def expandRDDbyMatch(x):
    res = []
    for match in x['matches']:
        tmp_dict = copy.copy(x)
        tmp_dict['word'] = match[u'_id']
        res.append(tmp_dict)
    return res


def percolateComment(x):
    es = Elasticsearch([ES_HOST])
    tmp_doc = {'doc': {'comment': x['body']}}
    percol_res = es.percolate(index="comment_percolators", doc_type="doctype", body=tmp_doc)
    x['percol_res'] = percol_res
    try:
        x['match_total'] = x['percol_res']['total']
        x['matches'] = x['percol_res']['matches']
        del x['percol_res']
    except KeyError:
        pass
    return x


def processRDD(rdd):
    last_rdd = (
        rdd.map(percolateComment)
           .filter(lambda x: x['match_total'] > 0)
           .flatMap(expandRDDbyMatch)
           .map(writeToCassandra)
    )
    last_rdd.count()


if __name__ == "__main__":
    conf = SparkConf().setAppName("Reddit Comment Cassandra Processor").setMaster(SPARK_MASTER)
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 2)
    brokers = KAFKA_BROKER_1 + ',' + KAFKA_BROKER_2 + ',' + KAFKA_BROKER_3
    topic = "reddit_comments"
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    comments = (
        kvs.map(lambda x: json.loads(x[1]))
           .filter(lambda r: len(r) > 0)
           .foreachRDD(processRDD)
    )

    ssc.start()
    ssc.awaitTermination()
