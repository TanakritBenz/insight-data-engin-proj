import sys
import time
import random
import json
import urllib2
from kafka import KafkaProducer


class Producer(object):

    def __init__(self, ip_addr):
        self.producer = KafkaProducer(bootstrap_servers=ip_addr, value_serializer=lambda m: json.dumps(m).encode('utf-8', errors='ignore'))

    def produce_msgs(self):
        f = lambda x: str(x) if isinstance(x, bool) else x
        msg_cnt = 1
        hdr = {'User-Agent': 'Kafka Producer'}
        req = urllib2.Request('https://www.reddit.com/r/all/comments/.json?limit=100', headers=hdr)
        while True:
            data = json.load(urllib2.urlopen(req))
            comments = data['data']['children']
            for c in comments:
                '''
                c['link_title'], #Post title
                c['body'], #Comment text
                c['author'], #Comment author
                c['subreddit'],
                c['parent_id'], #parent comment
                c['created_utc'], #epoch
                c['over_18'], #boolean
                c['ups'], #int
                c['downs'], #int
                c['controversiality'], #int
                c['gilded'], #int
                c['score'], #int
                '''
                c = c['data']
                wanted_keys = [u'id', u'link_title', u'body', u'author', u'subreddit',
                               u'parent_id', u'created_utc', u'over_18', u'ups', u'downs',
                               u'controversiality', u'gilded', u'score']
                msg = {wanted_keys: c[wanted_keys] for wanted_keys in wanted_keys}
                for m_tuple in msg.iteritems():
                    if isinstance(m_tuple[1], basestring):
                        msg[m_tuple[0]] = m_tuple[1].lower()
                self.producer.send('reddit_comments', msg)
                print msg_cnt, '=>', msg

                # msg_str = ''
                # for i, k in enumerate(wanted_keys):
                #     if k in [u'created_utc', u'ups', u'downs', u'controversiality', u'gilded', u'score']:
                #         c[k] = str(c[k])
                #     if k == u'over_18':
                #         c[k] = f(c[k])
                #     if i == len(wanted_keys)-1:
                #         msg_str += c[k].lower()
                #     else:
                #         msg_str += c[k].lower() + ';;'
                # self.producer.send('reddit_comments', msg_str.encode('utf-8', errors='ignore'))

                msg_cnt += 1
            time.sleep(2)

if __name__ == "__main__":
    args = sys.argv
    kafka_ips = ['52.34.22.125:9092', '52.34.64.163:9092', '52.33.253.180:9092']
    prod = Producer(kafka_ips)
    prod.produce_msgs()
