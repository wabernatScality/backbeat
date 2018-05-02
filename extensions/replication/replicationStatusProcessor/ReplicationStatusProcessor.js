'use strict'; // eslint-disable-line

const http = require('http');

const Logger = require('werelogs').Logger;
const errors = require('arsenal').errors;

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const RetryProducer = require('../../../lib/RetryProducer');
const VaultClientCache = require('../../../lib/clients/VaultClientCache');
const ReplicationTaskScheduler = require('../utils/ReplicationTaskScheduler');
const UpdateReplicationStatus = require('../tasks/UpdateReplicationStatus');
const QueueEntry = require('../../../lib/models/QueueEntry');
const ObjectQueueEntry = require('../utils/ObjectQueueEntry');

/**
 * @class ReplicationStatusProcessor
 *
 * @classdesc Background task that processes entries from the
 * replication status kafka queue and updates replication status on
 * source objects.
 */
class ReplicationStatusProcessor {

    /**
     * @constructor
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {Object} sourceConfig - source S3 configuration
     * @param {Object} sourceConfig.s3 - s3 endpoint configuration object
     * @param {Object} sourceConfig.auth - authentication info on source
     * @param {Object} repConfig - replication configuration object
     * @param {String} repConfig.replicationStatusTopic - replication
     *   status topic name
     * @param {String} repConfig.replicationStatusProcessor - config object
     *   specific to replication status processor
     * @param {String} repConfig.replicationStatusProcessor.groupId - kafka
     *   consumer group ID
     * @param {String} repConfig.replicationStatusProcessor.retryTimeoutS -
     *   number of seconds before giving up retries of an entry status
     *   update
     */
    constructor(kafkaConfig, sourceConfig, repConfig) {
        this.kafkaConfig = kafkaConfig;
        this.sourceConfig = sourceConfig;
        this.repConfig = repConfig;
        this._consumer = null;

        this.logger =
            new Logger('Backbeat:Replication:ReplicationStatusProcessor');

        // global variables
        // TODO: for SSL support, create HTTPS agents instead
        this.sourceHTTPAgent = new http.Agent({ keepAlive: true });

        this._setupVaultclientCache();

        this.taskScheduler = new ReplicationTaskScheduler(
            (ctx, done) => ctx.task.processQueueEntry(ctx.entry, done));
    }

    _setupVaultclientCache() {
        this.vaultclientCache = new VaultClientCache();

        if (this.sourceConfig.auth.type === 'role') {
            const { host, port } = this.sourceConfig.auth.vault;
            this.vaultclientCache
                .setHost('source:s3', host)
                .setPort('source:s3', port);
        }
    }

    getStateVars() {
        return {
            sourceConfig: this.sourceConfig,
            repConfig: this.repConfig,
            sourceHTTPAgent: this.sourceHTTPAgent,
            vaultclientCache: this.vaultclientCache,
            logger: this.logger,
        };
    }

    /**
     * Start kafka consumer
     *
     * @param {object} [options] - options object (only used for tests
     * for now)
     * @param {function} [cb] - optional callback called when startup
     * is complete
     * @return {undefined}
     */
    start(options, cb) {
        // TODO: Instantiate BackbeatProducer which will allow pushing to
        // the retry topic.
        this._retryProducer = new RetryProducer(this.kafkaConfig);
        this._consumer = new BackbeatConsumer({
            kafka: { hosts: this.kafkaConfig.hosts },
            topic: this.repConfig.replicationStatusTopic,
            groupId: this.repConfig.replicationStatusProcessor.groupId,
            concurrency:
            this.repConfig.replicationStatusProcessor.concurrency,
            queueProcessor: this.processKafkaEntry.bind(this),
            bootstrap: options && options.bootstrap,
        });
        this._consumer.on('error', () => {});
        this._consumer.on('ready', () => {
            this.logger.info('replication status processor is ready to ' +
                             'consume replication status entries');
            this._consumer.subscribe();
            if (cb) {
                return this._retryProducer.setupProducer(cb);
            }
            // TODO: How to handle if no callback in this method?
            this._retryProducer.setupProducer(console.log);
        });
    }

    /**
     * Stop kafka consumer and commit current offset
     *
     * @param {function} done - callback
     * @return {undefined}
     */
    stop(done) {
        if (!this._consumer) {
            return setImmediate(done);
        }
        return this._consumer.close(done);
    }

    /**
     * Set the Redis hash key for each failed backend.
     * @param {QueueEntry} queueEntry - The queue entry with the failed status.
     * @param {Object} kafkaEntry - The kafka entry with the failed status.
     * @param {Function} cb - The callback function
     * @return {undefined}
     */
    _setFailedKeys(queueEntry, kafkaEntry, cb) {
        const { status, backends } = queueEntry.getReplicationInfo();
        if (status !== 'FAILED') {
            return process.nextTick(cb);
        }
        let message;
        for (let i = 0; i < backends.length; i++) {
            const { status, site } = backends[i];
            if (status === 'FAILED' && site === queueEntry.getSite()) {
                const bucket = queueEntry.getBucket();
                const key = queueEntry.getObjectKey();
                const versionId = queueEntry.getEncodedVersionId();
                message = {
                    field: `${bucket}:${key}:${versionId}:${site}`,
                    value: Buffer.from(kafkaEntry.value).toString(),
                }
                break;
            }
        }
        if (message) {
            return this._retryProducer
                .publishRetryEntry(JSON.stringify(message), cb);
        }
        return cb();
    }

    /**
     * Proceed with updating the replication status of an object given
     * a kafka replication status queue entry
     *
     * @param {object} kafkaEntry - entry generated by the replication
     *   queue processor
     * @param {string} kafkaEntry.key - kafka entry key
     * @param {string} kafkaEntry.value - kafka entry value
     * @param {function} done - callback function
     * @return {undefined}
     */
    processKafkaEntry(kafkaEntry, done) {
        const sourceEntry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        if (sourceEntry.error) {
            this.logger.error('error processing source entry',
                              { error: sourceEntry.error });
            return process.nextTick(() => done(errors.InternalError));
        }
        let task;
        if (sourceEntry instanceof ObjectQueueEntry) {
            task = new UpdateReplicationStatus(this);
        }
        if (task) {
            // TODO: Use async series here.
            return this._setFailedKeys(sourceEntry, kafkaEntry, err => {
                if (err) {
                    this.logger.error('error setting redis hash key', {
                        error: err,
                    });
                }
                return this.taskScheduler.push({ task, entry: sourceEntry },
                    sourceEntry.getCanonicalKey(), done);
            });
        }
        this.logger.warn('skipping unknown source entry',
                         { entry: sourceEntry.getLogInfo() });
        return process.nextTick(done);
    }
}

module.exports = ReplicationStatusProcessor;
