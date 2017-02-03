var config = require('../config');

const cassandra = require('cassandra-driver');

const client = new cassandra.Client({
    contactPoints: config.cassandra.contactPoints,
    keyspace: config.cassandra.keyspace
});

client.connect(function(err, result) {
    if (err) {
        console.log('cassandra failed to connect: %s', err);
    } else {
        console.log('cassandra connected');
    }
});



module.exports = function(app) {
    app.get("/search/:topic/:lasttime", function(request, response) {
        var topic = request.params.topic;
        var lasttime = request.params.lasttime;
        var query = 'SELECT id, body, inserted_time, created_utc, created_utc_uuid FROM comments WHERE word = ? ORDER BY created_utc ASC LIMIT 2;';
        var params = [topic];
        if (lasttime !== 'unknown') {
            query = 'SELECT id, body, inserted_time, created_utc, created_utc_uuid FROM comments WHERE word = ? AND created_utc > ? ORDER BY created_utc ASC LIMIT 2;';
            params = [topic, lasttime];
        }

        console.log('Searching for "' + topic + '"...');

        client.execute(query, params, {prepare: true}, function(error, result) {
            if (error) {
                response.status(404).send({
                    'msg': error,
                });
            } else {
                response.json({
                    'response': result.rows,
                });
            }
        });
    });
};
