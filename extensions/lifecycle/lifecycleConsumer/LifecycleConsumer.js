'use strict'; // eslint-disable-line

const http = require('http');
const { EventEmitter } = require('events');
const Logger = require('werelogs').Logger;
const { errors } = require('arsenal');

const LifecycleObjectTask = require('../tasks/LifecycleObjectTask');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');

class LifecycleConsumer extends EventEmitter {

    /**
     * Create a kafka consumer to read lifecycle actions from queue
     * and perform them on a target S3 endpoint.
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {String} zkConfig.connectionString - zookeeper connection string
     *  as "host:port[/chroot]"
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {Object} lcConfig - lifecycle configuration object
     * @param {String} lcConfig.objectTasksTopic - lifecycle object topic name
     * @param {Object} lcConfig.consumer - kafka consumer object
     * @param {String} lcConfig.consumer.groupId - kafka consumer group id
     * @param {Number} lcConfig.consumer.retryTimeoutS - number of seconds
     *  before giving up retries of an entry lifecycle action
     * @param {Number} lcConfig.consumer.concurrency - number of max allowed
     *  concurrent operations
     * @param {Object} s3Config - S3 configuration
     * @param {Object} s3Config.host - s3 endpoint host
     * @param {Number} s3Config.port - s3 endpoint port
     * @param {Object} authConfig - authentication info on source
     * @param {String} [transport="http"] - transport method ("http"
     *  or "https")
     */
    constructor(zkConfig, kafkaConfig, lcConfig, s3Config, authConfig,
                transport = 'http') {
        super();
        this.zkConfig = zkConfig;
        this.kafkaConfig = kafkaConfig;
        this.lcConfig = lcConfig;
        this.s3Config = s3Config;
        this.authConfig = authConfig;
        this._transport = transport;
        this._consumer = null;

        this.logger = new Logger('Backbeat:Lifecycle:ObjectConsumer');

        // global variables
        // TODO: for SSL support, create HTTPS agents instead
        this.httpAgent = new http.Agent({ keepAlive: true });
    }


    /**
     * Start kafka consumer. Emits a 'ready' event when
     * consumer is ready.
     *
     * @return {undefined}
     */
    start() {
        this._consumer = new BackbeatConsumer({
            zookeeper: {
                connectionString: this.zkConfig.connectionString,
            },
            kafka: { hosts: this.kafkaConfig.hosts },
            topic: this.lcConfig.objectTasksTopic,
            groupId: this.lcConfig.consumer.groupId,
            concurrency: this.lcConfig.consumer.concurrency,
            queueProcessor: this.processKafkaEntry.bind(this),
        });
        this._consumer.on('error', () => {});
        this._consumer.on('ready', () => {
            this._consumer.subscribe();
            this.logger.info('lifecycle consumer successfully started');
            return this.emit('ready');
        });
    }


    /**
     * Proceed to the lifecycle action of an object given a kafka
     * object lifecycle queue entry
     *
     * @param {object} kafkaEntry - entry generated by the queue populator
     * @param {function} done - callback function
     * @return {undefined}
     */
    processKafkaEntry(kafkaEntry, done) {
        this.logger.debug('processing kafka entry');

        let entryData;
        try {
            entryData = JSON.parse(kafkaEntry.value);
        } catch (err) {
            this.logger.error('error processing lifecycle object entry',
                { error: err });
            return process.nextTick(() => done(errors.InternalError));
        }
        const task = new LifecycleObjectTask(this);
        return task.processQueueEntry(entryData, done);
    }

    getStateVars() {
        return {
            s3Config: this.s3Config,
            lcConfig: this.lcConfig,
            authConfig: this.authConfig,
            transport: this._transport,
            httpAgent: this.httpAgent,
            logger: this.logger,
        };
    }

}

module.exports = LifecycleConsumer;
