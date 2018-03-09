'use strict'; // eslint-disable-line strict

const async = require('async');
const Logger = require('werelogs').Logger;
const { RedisClient, StatsClient } = require('arsenal').metrics;

const StatsModel = require('./models/StatsModel');
const BackbeatConsumer = require('./BackbeatConsumer');
const redisKeys = require('../extensions/replication/constants').redisKeys;

// StatsClient constant defaults
const INTERVAL = 300; // 5 minutes;
const EXPIRY = 900; // 15 minutes

// BackbeatConsumer constant defaults
const CONSUMER_FETCH_MAX_BYTES = 5000020;
const CONCURRENCY = 10;

class MyStatsClient extends StatsClient {
    constructor(redisClient, interval, expiry) {
        super(redisClient, interval, expiry);
    }

    _normalizeTimestamp(d) {
        const m = d.getMinutes();
        return d.setMinutes(m - m % (Math.floor(this._interval / 60)), 0, 0);
    }

    /**
    * wrapper on `getStats` that handles a list of keys
    * @param {object} log - Werelogs request logger
    * @param {array} ids - service identifiers
    * @param {callback} cb - callback to call with the err/result
    * @return {undefined}
    */
    getAllStats(log, ids, cb) {
        if (!this._redis) {
            return cb(null, {});
        }

        const statsRes = {
            'requests': 0,
            '500s': 0,
            'sampleDuration': this._expiry,
        };
        let requests = 0;
        let errors = 0;

        // for now set concurrency to default of 10
        return async.eachLimit(ids, 10, (id, done) => {
            this.getStats(log, id, (err, res) => {
                if (err) {
                    return done(err);
                }
                requests += res.requests;
                errors += res['500s'];
                return done();
            });
        }, error => {
            if (error) {
                log.error('error getting stats', {
                    error,
                    method: 'StatsClient.getAllStats',
                });
                return cb(null, statsRes);
            }
            statsRes.requests = requests;
            statsRes['500s'] = errors;
            return cb(null, statsRes);
        });
    }
}

class MyRedisClient extends RedisClient {
    constructor(config, logger) {
        super(config, logger);
    }

    /**
    * scan a pattern and return matching keys
    * @param {string} pattern - string pattern to match with all existing keys
    * @param {number} count - scan count, defaults to 10
    * @param {callback} cb - callback (error, result)
    * @return {undefined}
    */
    scan(pattern, count = 10, cb) {
        const params = { match: pattern, count };
        const keys = [];

        const stream = this._client.scanStream(params);
        stream.on('data', resultKeys => {
            for (let i = 0; i < resultKeys.length; i++) {
                keys.push(resultKeys[i]);
            }
        });
        stream.on('end', () => {
            cb(null, keys);
        });
    }
}

class MetricsConsumer {
    /**
     * @constructor
     * @param {object} rConfig - redis configurations
     * @param {string} rConfig.host - redis host
     * @param {number} rConfig.port - redis port
     * @param {object} mConfig - metrics configurations
     * @param {string} mConfig.topic - metrics topic name
     * @param {object} zkConfig - zookeeper configurations
     * @param {string} zkConfig.connectionString - zookeeper connection string
     *   as "host:port[/chroot]"
     */
    constructor(rConfig, mConfig, zkConfig) {
        this.mConfig = mConfig;
        this.zkConfig = zkConfig;

        this.logger = new Logger('Backbeat:MetricsConsumer');

        const redisClient = new MyRedisClient(rConfig, this.logger);
        this._statsClient = new MyStatsClient(redisClient, INTERVAL,
            EXPIRY);
    }

    start() {
        const consumer = new BackbeatConsumer({
            zookeeper: { connectionString: this.zkConfig.connectionString },
            topic: this.mConfig.topic,
            groupId: 'backbeat-metrics-group',
            concurrency: CONCURRENCY,
            queueProcessor: this.processKafkaEntry.bind(this),
            fetchMaxBytes: CONSUMER_FETCH_MAX_BYTES,
        });
        consumer.on('error', () => {});
        consumer.subscribe();

        this.logger.info('metrics processor is ready to consume entries');
    }

    processKafkaEntry(kafkaEntry, done) {
        let data;
        try {
            data = JSON.parse(kafkaEntry.value);
        } catch (err) {
            this.logger.error('error processing metrics entry', {
                method: 'MetricsConsumer.processKafkaEntry',
                error: err,
            });
            return done();
        }
        /*
            data = {
                timestamp: 1509416671977,
                ops: 5,
                bytes: 195,
                extension: 'crr',
                type: 'processed'
            }
        */
        const site = data.site;
        if (data.type === 'processed') {
            this._sendRequest(`${redisKeys.opsDone}:${site}`, data.ops);
            this._sendRequest(`${redisKeys.bytesDone}:${site}`, data.bytes);
        } else if (data.type === 'queued') {
            this._sendRequest(`${redisKeys.ops}:${site}`, data.ops);
            this._sendRequest(`${redisKeys.bytes}:${site}`, data.bytes);
        } else {
            // unknown type
            this.logger.error('unknown type field encountered in metrics '
            + 'consumer', {
                method: 'MetricsConsumer.processKafkaEntry',
                dataType: data.type,
                data,
            });
        }
        return done();
    }

    _sendRequest(key, value) {
        this._statsClient.reportNewRequest(key, value);
    }
}

module.exports = MetricsConsumer;
