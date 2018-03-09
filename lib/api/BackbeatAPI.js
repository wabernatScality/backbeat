'use strict'; // eslint-disable-line strict

const async = require('async');
const zookeeper = require('node-zookeeper-client');

const { errors } = require('arsenal');
const { RedisClient, StatsClient } = require('arsenal').metrics;

const StatsModel = require('../models/StatsModel');
const BackbeatProducer = require('../BackbeatProducer');
const Healthcheck = require('./Healthcheck');
const routes = require('./routes');

// StatsClient constant defaults
// TODO: This should be moved to constants file
const INTERVAL = 300; // 5 minutes
const EXPIRY = 900; // 15 minutes

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

/**
 * Class representing Backbeat API endpoints and internals
 *
 * @class
 */
class BackbeatAPI {
    /**
     * @constructor
     * @param {object} config - configurations for setup
     * @param {werelogs.Logger} logger - Logger object
     */
    constructor(config, logger) {
        this._zkConfig = config.zookeeper;
        this._repConfig = config.extensions.replication;
        this._crrTopic = this._repConfig.topic;
        this._crrStatusTopic = this._repConfig.replicationStatusTopic;
        this._metricsTopic = config.metrics.topic;
        this._queuePopulator = config.queuePopulator;
        this._kafkaHost = config.kafka.hosts;
        this._redisConfig = config.redis;
        this._logger = logger;

        this._validSites = this._repConfig.destination.bootstrapList.map(
            item => (item.type ? item.type : item.site));

        this._crrProducer = null;
        this._crrStatusProducer = null;
        this._metricProducer = null;
        this._healthcheck = null;
        this._zkClient = null;

        // TODO: this should rely on the data stored in Redis and not an
        //  internal timer
        this._internalStart = Date.now();

        this._redisClient = new MyRedisClient(this._redisConfig, this._logger);
        this._statsClient = new MyStatsClient(this._redisClient, INTERVAL,
            EXPIRY);
    }

    /**
     * Check if incoming request is valid
     * @param {BackbeatRequest} bbRequest - holds relevant data about request
     * @return {boolean} true/false
     */
    isValidRoute(bbRequest) {
        const rDetails = bbRequest.getRouteDetails();
        if (!rDetails) {
            return false;
        }
        const route = bbRequest.getRoute();
        // first validate healthcheck routes
        if (route.substring(3) === 'healthcheck') {
            return true;
        }

        /*
            {
                extension: 'crr',
                site: 'my-site-name' || 'aws_s3',
                metric: '', (optional)
            }
        */
        // check metric routes
        // Are there any routes with matching extension?
        const extensions = routes.reduce((store, r) => {
            if (r.extensions) {
                store.push(Object.keys(r.extensions));
            }
            return store;
        }, []);
        if (![].concat.apply([], extensions).includes(rDetails.extension)) {
            return false;
        }

        let specifiedType;
        routes.forEach(r => {
            if (!r.extensions) {
                return;
            }
            if (!Object.keys(r.extensions).includes(rDetails.extension)) {
                return;
            }
            if (!r.extensions[rDetails.extension].includes(rDetails.site)) {
                return;
            }
            if (rDetails.metric && r.type === rDetails.metric) {
                specifiedType = r.type;
            }
        });

        // since this is an optional field, if a metric type was specified
        // in the route and it didn't match any metric types defined in
        // `routes.js`
        if (rDetails.metric && !specifiedType) {
            return false;
        }

        return true;
    }

    /**
     * Check if Zookeeper and Producer are connected
     * @return {boolean} true/false
     */
    isConnected() {
        return this._zkClient.getState().name === 'SYNC_CONNECTED'
            && this._checkProducersReady();
    }

    _checkProducersReady() {
        return this._crrProducer.isReady() && this._metricProducer.isReady()
            && this._crrStatusProducer.isReady();
    }

    /**
     * Get Kafka healthcheck
     * @param {object} details - route details from lib/api/routes.js
     * @param {function} cb - callback(error, data)
     * @return {undefined}
     */
    getHealthcheck(details, cb) {
        return this._healthcheck.getHealthcheck((err, data) => {
            if (err) {
                this._logger.error('error getting healthcheck', {
                    error: err.message,
                });
                return cb(errors.InternalError);
            }
            return cb(null, data);
        });
    }

    /**
     * Get data points which are the keys used to query Redis
     * @param {object} details - route details from lib/api/routes.js
     * @param {array} data - provides already fetched data in order of
     *   dataPoints mentioned for each route in lib/api/routes.js. This can be
     *   undefined.
     * @param {function} cb - callback(error, data), where data returns
     *   data stored in Redis.
     * @return {array} dataPoints array defined in lib/api/routes.js
     */
    _getData(details, data, cb) {
        if (!data) {
            const dataPoints = details.dataPoints;
            const site = details.site;
            return this._queryStats(dataPoints, site, cb);
        }
        return cb(null, data);
    }

    /**
     * Get replication backlog in ops count and size in MB
     * @param {object} details - route details from lib/api/routes.js
     * @param {function} cb - callback(error, data)
     * @param {array} data - optional field providing already fetched data
     *   in order of dataPoints mentioned for each route in lib/api/routes.js
     * @return {undefined}
     */
    getBacklog(details, cb, data) {
        this._getData(details, data, (err, res) => {
            if (err && err.type) {
                this._logger.error('error getting metric: backlog', {
                    origin: err.method,
                    method: 'BackbeatAPI.getBacklog',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error getting metric: backlog', {
                    method: 'BackbeatAPI.getBacklog',
                });
                return cb(errors.InternalError);
            }
            const d = res.map(r => r.requests);

            let opsBacklog = d[0] - d[1];
            if (opsBacklog < 0) opsBacklog = 0;
            let bytesBacklog = d[2] - d[3];
            if (bytesBacklog < 0) bytesBacklog = 0;
            const response = {
                backlog: {
                    description: 'Number of incomplete replication ' +
                        'operations (count) and number of incomplete MB ' +
                        'transferred (size)',
                    results: {
                        count: opsBacklog,
                        size: (bytesBacklog / 1000).toFixed(2),
                    },
                },
            };
            return cb(null, response);
        });
    }

    /**
     * Get completed replicated stats by ops count and size in MB
     * @param {object} details - route details from lib/api/routes.js
     * @param {function} cb - callback(error, data)
     * @param {array} data - optional field providing already fetched data
     *   in order of dataPoints mentioned for each route in lib/api/routes.js
     * @return {undefined}
     */
    getCompletions(details, cb, data) {
        this._getData(details, data, (err, res) => {
            if (err && err.type) {
                this._logger.error('error getting metric: completions', {
                    origin: err.method,
                    method: 'BackbeatAPI.getCompletions',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error getting metric: completions', {
                    method: 'BackbeatAPI.getCompletions',
                });
                return cb(errors.InternalError);
            }

            // Find if time since start is less than EXPIRY time
            const timeSinceStart = Math.floor(
                (Date.now() - this._internalStart) / 1000);
            // if timeSinceStart, then round the number to nearest 10's
            const timeDisplay = timeSinceStart < EXPIRY ?
                (Math.ceil(timeSinceStart / 10) * 10) : EXPIRY;

            const response = {
                completions: {
                    description: 'Number of completed replication operations ' +
                        '(count) and number of MB transferred (size) in the ' +
                        `last ${timeDisplay} seconds`,
                    results: {
                        count: res[0].requests,
                        size: (res[1].requests / 1000).toFixed(2),
                    },
                },
            };
            return cb(null, response);
        });
    }

    /**
     * Get current throughput in ops/sec and MB/sec
     * Throughput is the number of units processed in a given time
     * @param {object} details - route details from lib/api/routes.js
     * @param {function} cb - callback(error, data)
     * @param {array} data - optional field providing already fetched data
     *   in order of dataPoints mentioned for each route in lib/api/routes.js
     * @return {undefined}
     */
    getThroughput(details, cb, data) {
        this._getData(details, data, (err, res) => {
            if (err && err.type) {
                this._logger.error('error getting metric: throughput', {
                    origin: err.method,
                    method: 'BackbeatAPI.getThroughput',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            if (err) {
                this._logger.error('error getting metric: throughput', {
                    method: 'BackbeatAPI.getThroughput',
                });
                return cb(errors.InternalError);
            }

            const [opsThroughput, bytesThroughput] = res.map(r => {
                // Find if time since start is less than r.sampleDuration
                const timeSinceStart = Math.floor(
                    (Date.now() - this._internalStart) / 1000);
                if (timeSinceStart < r.sampleDuration) {
                    return (r.requests / timeSinceStart).toFixed(2);
                }
                return (r.requests / r.sampleDuration).toFixed(2);
            });

            const response = {
                throughput: {
                    description: 'Current throughput for replication ' +
                        'operations in ops/sec (count) and MB/sec (size)',
                    results: {
                        count: opsThroughput,
                        size: (bytesThroughput / 1000).toFixed(2),
                    },
                },
            };
            return cb(null, response);
        });
    }

    /**
     * Get all metrics
     * @param {object} details - route details from lib/api/routes.js
     * @param {function} cb = callback(error, data)
     * @param {array} data - optional field providing already fetched data
     *   in order of dataPoints mentioned for each route in lib/api/routes.js
     * @return {undefined}
     */
    getAllMetrics(details, cb, data) {
        this._getData(details, data, (err, res) => {
            if (err && err.type) {
                this._logger.error('error getting metric: all', {
                    origin: err.method,
                    method: 'BackbeatAPI.getAllMetrics',
                });
                return cb(err.type.customizeDescription(err.message));
            }
            if (err || res.length !== details.dataPoints.length) {
                this._logger.error('error getting metric: all', {
                    method: 'BackbeatAPI.getAllMetrics',
                });
                return cb(errors.InternalError);
            }
            // res = [ ops, ops_done, bytes, bytes_done ]
            return async.parallel([
                done => this.getBacklog({ dataPoints: new Array(4) }, done,
                    res),
                done => this.getCompletions({ dataPoints: new Array(2) }, done,
                    [res[1], res[3]]),
                done => this.getThroughput({ dataPoints: new Array(2) }, done,
                    [res[1], res[3]]),
            ], (err, results) => {
                if (err) {
                    this._logger.error('error getting metric: all', {
                        method: 'BackbeatAPI.getAllMetrics',
                    });
                    return cb(errors.InternalError);
                }
                const store = Object.assign({}, ...results);
                return cb(null, store);
            });
        });
    }

    /**
     * Query StatsClient for all ops given
     * @param {array} ops - array of redis key names to query
     * @param {string} site - site name or '*' wildcard
     * @param {function} cb - callback(err, res)
     * @return {undefined}
     */
    _queryStats(ops, site, cb) {
        return async.map(ops, (op, done) => {
            // Query all sites if given wildcard
            if (site === 'all') {
                const queryString = `*:${op}:*`;
                return this._redisClient.scan(queryString, undefined,
                (err, res) => {
                    if (err) {
                        // escalate error to log later
                        return done({
                            message: `Redis error: ${err.message}`,
                            type: errors.InternalError,
                            method: 'BackbeatAPI._queryStats',
                        });
                    }
                    const allKeys = res.map(key => {
                        const arr = key.split(':');
                        // Remove the "requests:<timestamp>" and process
                        return arr.slice(0, arr.length - 2).join(':');
                    });
                    const reducedKeys = [...new Set(allKeys)];

                    return this._statsClient.getAllStats(this._logger,
                        reducedKeys, done);
                });
            }
            // Query only a single given site or storage class
            // First, validate the site or storage class
            if (!this._validSites.includes(site)) {
                // escalate error to log later
                return done({
                    message: 'invalid site name provided',
                    type: errors.RouteNotFound,
                    method: 'BackbeatAPI._queryStats',
                });
            }
            const queryString = `${site}:${op}`;
            return this._statsClient.getStats(this._logger, queryString, done);
        }, cb);
    }

    /**
     * Setup internals
     * @param {function} cb - callback(error)
     * @return {undefined}
     */
    setupInternals(cb) {
        async.parallel([
            done => this._setZookeeper(done),
            done => this._setProducer(this._metricsTopic, (err, producer) => {
                if (err) {
                    return done(err);
                }
                this._metricProducer = producer;
                return done();
            }),
            done => this._setProducer(this._crrTopic, (err, producer) => {
                if (err) {
                    return done(err);
                }
                this._crrProducer = producer;
                return done();
            }),
            done => this._setProducer(this._crrStatusTopic, (err, producer) => {
                if (err) {
                    return done(err);
                }
                this._crrStatusProducer = producer;
                return done();
            }),
        ], err => {
            if (err) {
                this._logger.error('error setting up internal clients');
                return cb(err);
            }
            this._healthcheck = new Healthcheck(this._repConfig, this._zkClient,
                this._crrProducer, this._crrStatusProducer,
                this._metricProducer);
            this._logger.info('BackbeatAPI setup ready');
            return cb();
        });
    }

    _setProducer(topic, cb) {
        const producer = new BackbeatProducer({
            zookeeper: { connectionString: this._zkConfig.connectionString },
            topic,
        });

        producer.once('error', cb);
        producer.once('ready', () => {
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this._logger.error('error from backbeat producer', {
                    error: err,
                });
                return cb(err);
            });
            return cb(null, producer);
        });
    }

    _setZookeeper(cb) {
        const populatorZkPath = this._queuePopulator.zookeeperPath;
        const zookeeperUrl =
            `${this._zkConfig.connectionString}${populatorZkPath}`;

        const zkClient = zookeeper.createClient(zookeeperUrl, {
            autoCreateNamespace: this._zkConfig.autoCreateNamespace,
        });
        zkClient.connect();

        zkClient.once('error', cb);
        zkClient.once('connected', () => {
            zkClient.removeAllListeners('error');
            this._zkClient = zkClient;
            return cb();
        });
    }
}

module.exports = BackbeatAPI;
