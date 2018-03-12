const { EventEmitter } = require('events');
const { ConsumerGroup } = require('kafka-node');
const kafkaLogging = require('kafka-node/logging');
const assert = require('assert');
const async = require('async');
const joi = require('joi');

const BackbeatProducer = require('./BackbeatProducer');
const Logger = require('werelogs').Logger;
kafkaLogging.setLoggerProvider(new Logger('Consumer'));

const QueueEntry = require('./models/QueueEntry');

const CRR_TOPIC = require('../conf/Config').extensions.replication.topic;

// controls the number of messages to process in parallel
const CONCURRENCY_DEFAULT = 1;
const CLIENT_ID = 'BackbeatConsumer';

class BackbeatConsumer extends EventEmitter {

    /**
     * constructor
     * @param {Object} config - config
     * @param {string} config.topic - Kafka topic to subscribe to
     * @param {function} config.queueProcessor - function to invoke to process
     * an item in a queue
     * @param {string} config.groupId - consumer group id. Messages are
     * distributed among multiple consumers belonging to the same group
     * @param {Object} [config.zookeeper] - zookeeper endpoint config
     * @param {string} config.zookeeper.connectionString - zookeeper connection
     * string as "host:port[/chroot]"
     * @param {boolean} [config.ssl] - ssl enabled if ssl === true
     * @param {string} [config.fromOffset] - valid values latest/earliest/none
     * @param {number} [config.concurrency] - represents the number of entries
     * that can be processed in parallel
     * @param {number} [config.fetchMaxBytes] - max. bytes to fetch in a
     * fetch loop
     * @param {boolean} [config.createConsumer=true] - false to defer
     * creation of consumer (TEST ONLY, set to false before calling
     * {@link bootstrap()}, because for reliable behavior the consumer
     * has to be created after the producer - to ensure the topic is
     * created)
     */
    constructor(config) {
        super();

        const configJoi = {
            zookeeper: {
                connectionString: joi.string().required(),
            },
            ssl: joi.boolean(),
            topic: joi.string().required(),
            groupId: joi.string().required(),
            queueProcessor: joi.func(),
            fromOffset: joi.alternatives().try('latest', 'earliest', 'none'),
            concurrency: joi.number().greater(0).default(CONCURRENCY_DEFAULT),
            fetchMaxBytes: joi.number(),
            createConsumer: joi.boolean().default(true),
        };
        const validConfig = joi.attempt(config, configJoi,
                                        'invalid config params');

        const { zookeeper, ssl, topic, groupId, queueProcessor,
                fromOffset, concurrency, fetchMaxBytes,
                createConsumer } = validConfig;

        this._zookeeperEndpoint = zookeeper.connectionString;
        this._ssl = ssl;
        this._fromOffset = fromOffset;
        this._log = new Logger(CLIENT_ID);
        this._topic = topic;
        this._groupId = groupId;
        this._queueProcessor = queueProcessor;
        this._concurrency = concurrency;
        this._fetchMaxBytes = fetchMaxBytes;
        this._messagesConsumed = 0;

        // metrics - consumption
        this._metricsStore = {};

        this._consumer = null;
        if (createConsumer) {
            this._initConsumer();
        }
        return this;
    }

    _initConsumer() {
        this._consumer = new ConsumerGroup({
            host: this._zookeeperEndpoint,
            ssl: this._ssl,
            groupId: this._groupId,
            fromOffset: this._fromOffset,
            autoCommit: false,
            fetchMaxBytes: this._fetchMaxBytes,
        }, this._topic);
        this._consumer.once('connect', () => this.emit('connect'));
    }

    /**
    * subscribe to messages from a topic
    * Once subscribed, the consumer does a fetch from the topic with new
    * messages. Each fetch loop can contain one or more messages, so the fetch
    * is paused until the current queue of tasks are processed. Once the task
    * queue is empty, the current offset is committed and the fetch is resumed
    * to get the next batch of messages
    * @return {this} current instance
    */
    subscribe() {
        const q = async.queue(this._queueProcessor, this._concurrency);
        let partition = null;
        let offset = null;
        // consume a message in the fetch loop
        this._consumer.on('message', entry => {
            partition = entry.partition;
            offset = entry.offset;
            this._messagesConsumed++;
            this._consumer.pause();
            q.push(entry, err => {
                this._log.debug('finished processing of consumed entry', {
                    method: 'BackbeatConsumer.subscribe',
                    partition,
                    offset,
                });
                if (err) {
                    this._log.error('error processing an entry', {
                        error: err,
                        method: 'BackbeatConsumer.subscribe',
                        partition,
                        offset,
                    });
                    this.emit('error', err, entry);
                } else if (entry.topic === CRR_TOPIC &&
                this._topic === CRR_TOPIC) {
                    this._incrementMetrics(entry);
                }
            });
        });

        // commit offset and resume fetch loop when the task queue is empty
        q.drain = () => {
            const count = this._messagesConsumed;
            this._consumer.sendOffsetCommitRequest([{
                topic: this._topic,
                partition, // default 0
                offset,
                metadata: 'm', //default 'm'
            }], () => {
                this.emit('consumed', count);
                this._messagesConsumed = 0;

                this.emit('metrics', this._metricsStore);
                this._metricsStore = {};

                this._consumer.resume();
            });
        };

        this._consumer.on('error', error => {
            this._log.error('error subscribing to topic', {
                error,
                method: 'BackbeatConsumer.subscribe',
                topic: this._topic,
                partition: this._partition,
            });
            this.emit('error', error);
        });

        return this;
    }

    _incrementMetrics(entry) {
        const qEntry = QueueEntry.createFromKafkaEntry(entry);
        console.log('--- got a crr topic entry in consumer ---')
        console.log(qEntry)
        console.log('entry rep info:', qEntry.getReplicationInfo());
        if (!qEntry.error && qEntry.getReducedLocations) {
            const locations = qEntry.getReducedLocations();
            const bytes = locations.reduce((sum, item) =>
                sum + item.size, 0);

            const repSites = qEntry.getReplicationInfo().backends;
            const sites = repSites.reduce((store, entry) => {
                if (entry.status === 'PENDING') {
                    store.push(entry.site);
                }
                return store;
            }, []);

            // for one-to-many
            sites.forEach(site => {
                if (!this._metricsStore[site]) {
                    this._metricsStore[site] = {
                        ops: 1,
                        bytes,
                    };
                } else {
                    this._metricsStore[site].ops++;
                    this._metricsStore[site].bytes += bytes;
                }
            });
        }
    }

    /**
     * Bootstrap consumer by periodically sending bootstrap messages
     * and wait until it's receiving newly produced messages in a
     * timely fashion. The constructor must have been called with
     * "createConsumer" parameter set to false.
     *
     * ONLY USE FOR TESTING PURPOSE
     *
     * @param {function} cb - callback when consumer is effectively
     * receiving newly produced messages
     * @return {undefined}
     */
    bootstrap(cb) {
        let lastBootstrapId;
        let producer; // eslint-disable-line prefer-const
        let timer; // eslint-disable-line prefer-const
        function onBootstrapMessage(message) {
            const bootstrapId = JSON.parse(message.value).bootstrapId;
            if (bootstrapId) {
                this._log.info('bootstraping backbeat consumer: ' +
                               'received bootstrap message',
                               { bootstrapId });
                if (bootstrapId === lastBootstrapId) {
                    this._log.info('backbeat consumer is bootstrapped');
                    clearInterval(timer);
                    this._consumer.removeListener('message',
                                                  onBootstrapMessage);
                    producer.close(cb);
                }
            }
        }
        assert.strictEqual(this._consumer, null);
        producer = new BackbeatProducer({
            zookeeper: { connectionString: 'localhost:2181' },
            topic: this._topic,
        });
        timer = setInterval(() => {
            lastBootstrapId = `${Math.round(Math.random() * 1000000)}`;
            const contents = `{"bootstrapId":"${lastBootstrapId}"}`;
            this._log.info('bootstraping backbeat consumer: ' +
                           'sending bootstrap message',
                           { contents });
            producer.send([{ key: 'bootstrap',
                             message: contents }],
                          () => {});
            if (!this._consumer) {
                setTimeout(() => {
                    this._initConsumer();
                    this._consumer.on('message', onBootstrapMessage.bind(this));
                }, 500);
            }
        }, 1000);
    }

    /**
    * force commit the current offset and close the client connection
    * @param {callback} cb - callback to invoke
    * @return {undefined}
    */
    close(cb) {
        this._consumer.close(true, cb);
    }
}

module.exports = BackbeatConsumer;
