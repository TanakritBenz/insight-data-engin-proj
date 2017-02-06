import sys
import time
import json
import urllib2
from kafka import KafkaProducer
from os import path
sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )
from config import KAFKA_BROKER_1, KAFKA_BROKER_2, KAFKA_BROKER_3


class Producer(object):

    def __init__(self, ip_addr):
        self.producer = KafkaProducer(bootstrap_servers=ip_addr, value_serializer=lambda m: json.dumps(m).encode('utf-8', errors='ignore'))

    def produce_msgs(self):
        msg_cnt = 1
        hdr = {'User-Agent': 'Kafka Producer'}
        req = urllib2.Request('https://www.reddit.com/r/all/new/.json?limit=100', headers=hdr)
        while True:
            data = json.load(urllib2.urlopen(req))
            comments = data['data']['children']
            for c in comments:
                c = c['data']
                '''
                c['id'],
                c['title'],
                c['permalink'],
                c['url'],
                c['author'],
                c['subreddit'],
                c['created_utc'], #epoch
                c['over_18'], #boolean
                c['ups'], #int
                c['downs'], #int
                c['gilded'], #int
                c['score'], #int
                '''
                wanted_keys = [u'id', u'title', u'permalink', u'url',
                               u'author', u'subreddit', u'created_utc',
                               u'over_18', u'ups', u'downs', u'gilded',
                               u'score']
                msg = {wanted_keys: c[wanted_keys] for wanted_keys in wanted_keys}
                for m_tuple in msg.iteritems():
                    if isinstance(m_tuple[1], basestring):
                        msg[m_tuple[0]] = m_tuple[1].lower()
                self.producer.send('reddit-posts', msg)
                print msg_cnt, '=>', msg

                msg_cnt += 1
            time.sleep(2)

if __name__ == "__main__":
    args = sys.argv
    kafka_ips = [KAFKA_BROKER_1, KAFKA_BROKER_2, KAFKA_BROKER_3]
    prod = Producer(kafka_ips)
    prod.produce_msgs()
