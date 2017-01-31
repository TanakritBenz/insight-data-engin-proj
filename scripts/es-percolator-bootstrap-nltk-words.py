import sys
import pprint
from elasticsearch import Elasticsearch
from nltk.corpus import words, stopwords
from os import path
sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )
from config import ES_HOST

es = Elasticsearch([ES_HOST])
mapping = {
    "mappings": {
        "doctype": {
            "properties": {
                "comment": {
                    "type": "text"
                }
            }
        },
        "queries": {
            "properties": {
                "query": {
                    "type": "percolator"
                }
            }
        }
    }
}
es.indices.create(index='comment_percolators', body=mapping, ignore=400)

stop_word_list = stopwords.words('english')
stop_word_set = set(stop_word_list) #To quickly test if a word is not a stop word, use a set
word_list = words.words()
for i, word in enumerate(word_list):
    word = word.lower()
    if word not in stop_word_set:
        query = {
            "query": {
                "match": {
                    "comment": word
                }
            }
        }
        res = es.index(index='comment_percolators', doc_type='queries', body=query, id=word)
        print('word #'+str(i))
        pprint.pprint(res)


# doc1 = {'doc': {'comment': 'this is something about python'}}
# res = es.percolate(index="comment_percolators", doc_type="doctype", body=doc1)
# pprint.pprint(res)

# doc2 = {'doc': {'comment': 'this is another piece of text'}}
# res = es.percolate(index="comment_percolators", doc_type="doctype", body=doc2)
# pprint.pprint(res)
