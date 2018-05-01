'use strict'; // eslint-disable-line strict

const Logger = require('werelogs').Logger;
const redisClient = require('../extensions/replication/utils/getRedisClient')();

const BackbeatConsumer = require('./BackbeatConsumer');
const redisKeys = require('../extensions/replication/constants').redisKeys;

// StatsClient constant defaults
const INTERVAL = 300; // 5 minutes;
const EXPIRY = 900; // 15 minutes

// BackbeatConsumer constant defaults
const CONSUMER_FETCH_MAX_BYTES = 5000020;
const CONCURRENCY = 10;

class RetryConsumer {
    /**
     * @constructor
     * @param {object} rConfig - redis configurations
     * @param {string} rConfig.host - redis host
     * @param {number} rConfig.port - redis port
     * @param {object} mConfig - metrics configurations
     * @param {string} mConfig.topic - metrics topic name
     * @param {object} kafkaConfig - kafka configurations
     * @param {string} kafkaConfig.hosts - kafka hosts
     *   as "host:port[/chroot]"
     */
    constructor(kafkaConfig) {
        this._kafkaConfig = kafkaConfig;
        // TODO: Do not use hard coded value.
        this._topic = 'backbeat-replication-retry-2';
        this.kafkaConfig = kafkaConfig;

        this.logger = new Logger('Backbeat:RetryConsumer');
    }

    start() {
        const consumer = new BackbeatConsumer({
            kafka: { hosts: this.kafkaConfig.hosts },
            topic: 'backbeat-replication-retry-2',
            groupId: 'backbeat-retry-group',
            concurrency: CONCURRENCY,
            queueProcessor: this.processKafkaEntry.bind(this),
            fetchMaxBytes: CONSUMER_FETCH_MAX_BYTES,
        });
        consumer.on('error', () => {});
        consumer.on('ready', () => {
            consumer.subscribe();
            this.logger.info('retry consumer is ready to consume entries');
        });
    }

    processKafkaEntry(kafkaEntry, done) {
        console.log('CONSUMED RETRY ENTRY FROM TOPIC');
        const log = this.logger.newRequestLogger();
        let data;
        try {
            data = JSON.parse(kafkaEntry.value);
        } catch (err) {
            log.error('error processing metrics entry', {
                method: 'RetryConsumer.processKafkaEntry',
                error: err,
            });
            log.end();
            return done();
        }
        const fields = [];
        for (let i = 0; i < data.length; i += 2) {
            fields.push(data[i], Buffer(data[i + 1]).toString());
        }
        const cmds = ['hmset', redisKeys.failedCRR, ...fields];
        return redisClient.batch([cmds], (err, res) => {
            if (err) {
                return done(err);
            }
            const [cmdErr] = res[0];
            return done(cmdErr);
        });
    }

    _sendRequest(key, value) {
        this._statsClient.reportNewRequest(key, value);
    }
}

module.exports = RetryConsumer;
