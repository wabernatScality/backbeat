'use strict'; // eslint-disable-line

const async = require('async');
const schedule = require('node-schedule');
const zookeeper = require('node-zookeeper-client');

const Logger = require('werelogs').Logger;

const BackbeatProducer = require('../../../lib/BackbeatProducer');
const zookeeperHelper = require('../../../lib/clients/zookeeper');

const DEFAULT_CRON_RULE = '* * * * *';
const DEFAULT_CONCURRENCY = 10;

/**
 * @class LifecycleConductor
 *
 * @classdesc Background task that periodically reads the lifecycled
 * buckets list on Zookeeper and creates bucket listing tasks on
 * Kafka.
 */
class LifecycleConductor {

    /**
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {string} zkConfig.connectionString - zookeeper connection string
     *   as "host:port[/chroot]"
     * @param {Object} lcConfig - lifecycle configuration object
     * @param {String} lcConfig.bucketTasksTopic - lifecycle
     *   bucket tasks topic name
     * @param {String} lcConfig.conductor - config object specific to
     *   lifecycle conductor
     * @param {String} [lcConfig.conductor.cronRule="* * * * *"] -
     *   cron rule for bucket processing periodic task
     * @param {Number} [lcConfig.conductor.concurrency=10] - maximum
     *   number of concurrent bucket-to-kafka operations allowed
     */
    constructor(zkConfig, lcConfig) {
        this.zkConfig = zkConfig;
        this.lcConfig = lcConfig;
        this._cronRule =
            this.lcConfig.conductor.cronRule || DEFAULT_CRON_RULE;
        this._concurrency =
            this.lcConfig.conductor.concurrency || DEFAULT_CONCURRENCY;
        this._producer = null;
        this._zkClient = null;
        this._cronJob = null;

        this.logger = new Logger('Backbeat:Lifecycle:Conductor');
    }

    getBucketsZkPath() {
        return `${this.lcConfig.zookeeperPath}/data/buckets`;
    }

    getQueuedBucketsZkPath() {
        return `${this.lcConfig.zookeeperPath}/run/queuedBuckets`;
    }

    getConductorLockZkPath() {
        return `${this.lcConfig.zookeeperPath}/run/conductorLock`;
    }

    initZkPaths(cb) {
        async.each([this.getBucketsZkPath(),
                    this.getQueuedBucketsZkPath()],
                   (path, done) => this._zkClient.mkdirp(path, done), cb);
    }

    _processBucket(ownerId, bucketName, done) {
        this.logger.debug('processing bucket', { ownerId, bucketName });
        const zkPath =
                  `${this.getQueuedBucketsZkPath()}/${ownerId}:${bucketName}`;
        this._zkClient.create(
            zkPath,
            null,
            zookeeper.ACL.OPEN_ACL_UNSAFE,
            zookeeper.CreateMode.EPHEMERAL,
            err => {
                if (err) {
                    if (err.getCode() ===
                        zookeeper.Exception.NODE_EXISTS) {
                        this.logger.debug(
                            'bucket processing already queued',
                            { ownerId, bucketName });
                        return done(null, []); // no new kafka message
                    }
                    this.logger.error(
                        'error creating zookeeper node',
                        { zkPath, error: err.message });
                    return done(err);
                }
                return done(null, [{
                    message: JSON.stringify({
                        action: 'processObjects',
                        target: {
                            bucket: bucketName,
                            owner: ownerId,
                        },
                        details: {},
                    }),
                }]);
            });
    }

    processBuckets() {
        const zkLockPath = this.getConductorLockZkPath();
        const zkBucketsPath = this.getBucketsZkPath();
        let lockHeld = false;
        async.waterfall([
            next => this._zkClient.create(
                zkLockPath,
                null,
                zookeeper.ACL.OPEN_ACL_UNSAFE,
                zookeeper.CreateMode.EPHEMERAL,
                err => {
                    if (err) {
                        if (err.getCode() ===
                            zookeeper.Exception.NODE_EXISTS) {
                            this.logger.debug(
                                'bucket processing in progress, skipping');
                        } else {
                            this.logger.error(
                                'error creating conductor lock on zookeeper',
                                { zkPath: zkLockPath, error: err.message });
                        }
                    } else {
                        lockHeld = true;
                    }
                    return next(err);
                }),
            next => this._zkClient.getChildren(
                zkBucketsPath,
                null,
                (err, buckets) => {
                    if (err) {
                        this.logger.error(
                            'error getting list of buckets from zookeeper',
                            { zkPath: zkBucketsPath, error: err.message });
                    }
                    return next(err, buckets);
                }),
            (buckets, next) => async.concatLimit(
                buckets, this._concurrency,
                (bucket, done) => {
                    const [ownerId, bucketName] = bucket.split(':');
                    if (!ownerId || !bucketName) {
                        this.logger.error(
                            'malformed zookeeper bucket entry, skipping',
                            { zkPath: zkBucketsPath, bucket });
                        return process.nextTick(done);
                    }
                    return this._processBucket(ownerId, bucketName, done);
                }, next),
            (entries, next) => {
                this.logger.debug(
                    'producing kafka entries',
                    { topic: this.lcConfig.bucketTasksTopic,
                      entries });
                if (entries.length === 0) {
                    return next();
                }
                return this._producer.send(entries, next);
            },
        ], () => {
            if (lockHeld) {
                this._zkClient.remove(zkLockPath, -1, err => {
                    if (err) {
                        this.logger.error(
                            'error removing conductor lock on zookeeper',
                            { zkPath: zkLockPath, error: err.message });
                    }
                });
            }
        });
    }

    /**
     * Initialize kafka producer and zookeeper client
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    init(done) {
        if (this._zkClient) {
            // already initialized
            return process.nextTick(done);
        }
        return async.series([
            next => {
                const producer = new BackbeatProducer({
                    zookeeper: { connectionString:
                                 this.zkConfig.connectionString },
                    topic: this.lcConfig.bucketTasksTopic,
                    keyedPartitioner: false,
                });
                producer.once('error', next);
                producer.once('ready', () => {
                    producer.removeAllListeners('error');
                    producer.on('error', err => {
                        this.logger.error('error from backbeat producer', {
                            topic: this.lcConfig.bucketTasksTopic,
                            error: err,
                        });
                    });
                    this._producer = producer;
                    next();
                });
            },
            next => {
                this._zkClient = zookeeperHelper.createClient(
                    this.zkConfig.connectionString);
                this._zkClient.connect();
                this._zkClient.once('error', next);
                this._zkClient.once('ready', () => {
                    // just in case there would be more 'error' events
                    // emitted
                    this._zkClient.removeAllListeners('error');
                    this._zkClient.on('error', err => {
                        this.logger.error(
                            'error from lifecycle conductor zookeeper client',
                            { error: err });
                    });
                    next();
                });
            },
        ], done);
    }

    /**
     * Start the cron jobkafka producer
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    start(done) {
        if (this._cronJob) {
            // already started
            return process.nextTick(done);
        }
        return async.series([
            next => this.init(next),
            next => {
                this._cronJob = schedule.scheduleJob(
                    this._cronRule,
                    this.processBuckets.bind(this));
                process.nextTick(next);
            },
        ], done);
    }

    /**
     * Stop cron task (if started), stop kafka consumer and commit
     * current offset
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    stop(done) {
        if (this._cronJob) {
            this._cronJob.cancel();
            this._cronJob = null;
        }
        if (!this._producer) {
            return process.nextTick(done);
        }
        return this._producer.close(() => {
            this._producer = null;
            this._zkClient = null;
            done();
        });
    }
}

module.exports = LifecycleConductor;
