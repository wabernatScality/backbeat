'use strict'; // eslint-disable-line strict

const async = require('async');
const { Logger } = require('werelogs');

const BackbeatProducer = require('./BackbeatProducer');
const MetricsModel = require('./models/MetricsModel');

class MetricsProducer {
    /**
    * @constructor
    * @param {Object} zkConfig - zookeeper configurations
    * @param {Object} mConfig - metrics configurations
    */
    constructor(zkConfig, mConfig) {
        this._zkConfig = zkConfig;
        this._topic = mConfig.topic;

        this._producer = null;
        this._log = new Logger('MetricsProducer');
    }

    setupProducer(done) {
        const producer = new BackbeatProducer({
            zookeeper: { connectionString: this._zkConfig.connectionString },
            topic: this._topic,
        });
        producer.once('error', done);
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this._log.error('error from backbeat producer',
                               { error: err });
            });
            this._producer = producer;
            done();
        });
    }

    /**
     * @param {Object} extMetrics - an object where keys are all buckets for a
     *   given extension and values are the metrics for the bucket
     *   (i.e. { my-bucket: { ops: 1, bytes: 124 } } )
     * @param {String} type - type of metric (queueud or processed)
     * @param {String} ext - extension (i.e. 'crr')
     * @param {function} cb - callback
     * @return {undefined}
     */
    publishMetrics(extMetrics, type, ext, cb) {
        async.each(Object.keys(extMetrics), (bucketName, done) => {
            const { ops, bytes } = extMetrics[bucketName];
            const message = new MetricsModel(ops, bytes, ext, type,
                bucketName).serialize();
            this._producer.send([{ message }], err => {
                if (err) {
                    // Using trace here because errors are already logged in
                    // BackbeatProducer. This is to log to see source of caller
                    this._log.trace(`error publishing ${type} metrics for` +
                        `extension metrics ${ext}`);
                }
                done();
            });
        }, cb);
    }
}

module.exports = MetricsProducer;