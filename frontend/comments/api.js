'use strict';

const express = require('express');
const bodyParser = require('body-parser');
const cassandra = require('cassandra-driver');
const router = express.Router();

router.use(bodyParser.json());

const client = new cassandra.Client({
    contactPoints: ['ec2-52-34-22-125.us-west-2.compute.amazonaws.com',
                    'ec2-52-33-253-180.us-west-2.compute.amazonaws.com',
                    'ec2-52-34-64-163.us-west-2.compute.amazonaws.com'],
    keyspace: 'reddit_comments'
});
client.connect(function(err, result) {
    consolde.log('cassandra connected')
});

const query = 'SELECT * FROM word_time_json WHERE word = ? ORDER BY created_utc_uuid DESC LIMIT 10;';
var params = ['rip'];

router.get('/search', function(req, res) {
    client.execute(query, params, {
        prepare: true
    }, function(err, result) {
        console.log('result => %s', result);
        if (err) {
            res.status(404).send({
                msg: err
            });
        } else {
            res.json({
                response: result,
            });
        }
    });
});
