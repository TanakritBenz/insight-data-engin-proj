var config = require('../config');
var elasticsearch = require('elasticsearch');
var cassandra = require('cassandra-driver');
var cassandra_client = new cassandra.Client({
    contactPoints: config.cassandra.contactPoints,
    keyspace: config.cassandra.keyspace
});

cassandra_client.connect(function(err, result) {
    if (err) {
        console.log('cassandra failed to connect: %s', err);
    } else {
        console.log('cassandra connected');
    }
});


var es_client = new elasticsearch.Client({
    host: config.es.host,
    // log: 'trace'
});
es_client.ping({
    requestTimeout: 10000,
}, function(error) {
    if (error) {
        console.error('elasticsearch cluster is down!');
    } else {
        console.log('elasticsearch is all well');
    }
});


var registerQuery = function(topic, cb) {
    /*es_client.indices.create({
        index: 'post_percolators',
        body: {
            "mappings": {
                "doctype": {
                    "properties": {
                        "post": {
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
        },
        ignore: [404]
    }).then(function(body) {
        // since we told the client to ignore 404 errors, the
        // promise is resolved even if the index already exists
        console.log("Created 'post_percolators' in es");
    }, function(error) {});*/

    es_client.index({
        index: 'post_percolators',
        type: 'doctype',
        id: topic,
        body: {
            "doc": {
                "match_phrase": {
                    "post": topic
                }
            }
        },
    }).then(function(resp) {
        var hits = resp.hits.hits;
        console.log('es search result: ', hits);
        cb(hits);
    }, function(err) {
        console.trace(err.message);
        cb(err.message);
    });
};



module.exports = function(app) {
    app.get("/get_graph_data/:topic", function(request, response) {
        var topic = request.params.topic;
        query = 'SELECT query, toUnixTimestamp(time_utc), ups, downs FROM posts_agg WHERE query = ? ORDER BY time_utc ASC;';
        params = [topic];
        cassandra_client.execute(query, params, {prepare: true}, function(error, result) {
            if (error) {
                response.status(404).send({
                    'msg': error,
                });
            } else {
                response.json({
                    'res': result.rows,
                });
            }
        });
    });

    app.get("/register_query/:query", function(request, response) {
        var query = request.params.query;
        console.log('Registering ES percolator: ', query);
        es_client.index({
            index: 'post_percolators',
            type: 'queries',
            id: query,
            body: {
                "query": {
                    "match_phrase": {
                        "post": query
                    }
                }
            },
        }).then(function(resp) {
            console.log('ES result: ', resp);
            response.json({
                'res': resp,
            });
        }, function(err) {
            console.trace(err.message);
            response.status(404).send({
                'msg': err.message,
            });
        });


    });

    app.get("/search/:topic/:lasttime", function(request, response) {
        var topic = request.params.topic;
        var lasttime = request.params.lasttime;
        var query = 'SELECT query, created_utc, doc_id, title, permalink, url, author, subreddit, ups, downs FROM posts WHERE query=? AND created_utc > ? ORDER BY created_utc DESC;';
        var params = [topic, lasttime];

        if (lasttime ==='unknown') {
            query = 'SELECT query, created_utc, doc_id, title, permalink, url, author, subreddit, ups, downs FROM posts WHERE query=? ORDER BY created_utc DESC LIMIT 2;';
            params = [topic];
        }
        // console.log(query, params);
        cassandra_client.execute(query, params, {prepare: true}, function(error, result) {
            if (error) {
                response.status(404).send({
                    'msg': error,
                });
            } else {

                response.json({
                    'res': result.rows,
                });
            }
        });
    });
};
