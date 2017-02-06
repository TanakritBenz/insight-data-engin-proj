from __future__ import print_function
import sys, json, copy, time, pprint, datetime
import pyspark_cassandra
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.util import Date
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from os import path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from config import ES_HOST, CASSANDRA_HOST, SPARK_MASTER, KAFKA_BROKER_1, KAFKA_BROKER_2, KAFKA_BROKER_3


def limitMatchesAt5(x):
    res = []
    if x[1]['match_total'] > 5:
        x[1]['match_total'] = 5
        x[1]['matches'] = x[1]['matches'][:5]
    res.append( (x[0], x[1]['matches'], x[1]) )
    return res

def percolatePost(x):
    es = Elasticsearch([ES_HOST])
    tmp_doc = {'doc': {'post': x[1]['title']}}
    percol_res = es.percolate(index="post_percolators", doc_type="doctype", body=tmp_doc)
    x[1]['percol_res'] = percol_res
    try:
        x[1]['match_total'] = x[1]['percol_res']['total']
        x[1]['matches'] = x[1]['percol_res']['matches']
        del x[1]['percol_res']
    except KeyError:
        pass
    return x

def formatForCassandraPostsCountTable(x):
    tmp = x[0].split('_')
    x = {
        'word': tmp[0],
        'date_created': Date(datetime.datetime.utcfromtimestamp(tmp[1]).date()).seconds,
        'ups': x[1]
    }
    return x

def expandRDDbyMatch(x):
    res = []
    for match in x[2]['matches']:
        t = Date(datetime.datetime.utcfromtimestamp(x[2]['created_utc']).date()).seconds
        res.append( (match[u'_id'] + '_' + str(t), x[2]['ups']) )
    return res

def fetchCassandra(words):
    return_rdd = []
    # build a list of futures
    futures = []
    query = "SELECT word, toUnixTimestamp(date_created), ups FROM upvote_counts WHERE word=%s"
    for word in words:
        futures.append(session.execute_async(query, [word]))

    def handle_success(rows):
        record = rows[0]
        try:
            return_rdd.append( (record.word + '_' + str(record.date_created), record.ups) )
        except Exception:
            print("Failed to process record %s", record.id)
            # don't re-raise errors in the callback

    def handle_error(exception):
        print("Failed to fetch record info: %s", exception)

    # wait for them to complete and use the results
    for future in futures:
        future.add_callbacks(handle_success, handle_error)
    return_rdd = sc.parallelize(return_rdd)
    return return_rdd


def processRDD(rdd):
    start_time = time.time()

    rdd = (
        rdd.map(lambda x: (x['id'], x))
           .reduceByKey(lambda x,y: x) #discard similar posts
           .filter(lambda x: x[1]['over_18'] == False)
           .map(percolatePost)
           .filter(lambda x: x[1]['match_total'] > 0)
           .flatMap(limitMatchesAt5)
    )
    print('OG RDD --->', rdd.count(), rdd.take(3))
    all_words = rdd.flatMap(lambda x: x[1]).map(lambda x: x[u'_id']).distinct()
    rdd = rdd.flatMap(expandRDDbyMatch).reduceByKey(lambda x,y: x+y)
    print('RETRO RDD --->', rdd.count(), rdd.take(3))
    print('ALL WORDS --->', all_words.collect())
    rdd_from_db = fetchCassandra(all_words.collect())
    print('Fresh from DB --->', rdd_from_db.count(), rdd_from_db.take(3))

    # .map(formatForCassandraPostsCountTable)


    # print('#posts processed =>', rdd.count())

    # rdd.saveToCassandra(
    #     keyspace="reddit",
    #     table="upvote_counts",
    #     consistency_level=ConsistencyLevel.ANY,
    #     batch_grouping_key='partition',
    #     ttl=300000
    # )

    print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == "__main__":
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect('reddit')
    conf = SparkConf().setAppName("Reddit Posts Counting Process").setMaster(SPARK_MASTER)
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 2)
    brokers = KAFKA_BROKER_1 + ',' + KAFKA_BROKER_2 + ',' + KAFKA_BROKER_3
    topic = "reddit-posts"
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    posts = (
        kvs.map(lambda x: json.loads(x[1]))
           .filter(lambda r: len(r) > 0)
           .foreachRDD(processRDD)
    )

    ssc.start()
    ssc.awaitTermination()
