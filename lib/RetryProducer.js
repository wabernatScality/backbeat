'use strict'; // eslint-disable-line strict

const async = require('async');
const { Logger } = require('werelogs');

const BackbeatProducer = require('./BackbeatProducer');
const MetricsModel = require('./models/MetricsModel');

class RetryProducer {
    constructor(kafkaConfig) {
        this._kafkaConfig = kafkaConfig;
        // TODO: Do not use hard coded value.
        this._topic = 'backbeat-replication-retry-3';

        this._producer = null;
        this._log = new Logger('RetryProducer');
    }

    setupProducer(done) {
        const producer = new BackbeatProducer({
            kafka: { hosts: this._kafkaConfig.hosts },
            topic: this._topic,
        });
        producer.once('error', done);
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            producer.on('error', err =>
                this._log.error('error from backbeat producer', {
                    error: err
                }));
            this._producer = producer;
            done();
        });
    }

    publishRetryEntry(message, cb) {
        this._producer.send([{ message }], err => {
            if (err) {
                this._log.trace(`error publishing retry entry`);
            };
            return cb();
        });
    }
}

module.exports = RetryProducer;
