const { EventEmitter } = require('events');
const kafka = require('node-rdkafka');
const assert = require('assert');
const async = require('async');
const joi = require('joi');

const BackbeatProducer = require('./BackbeatProducer');
const Logger = require('werelogs').Logger;

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
     * @param {string} [config.zookeeper.connectionString] - zookeeper
     * connection string as "host:port[/chroot]"
     * @param {Object} config.kafka - kafka connection config
     * @param {string} config.kafka.hosts - kafka hosts list
     * as "host:port[,host:port...]"
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
                connectionString: joi.string(),
            },
            kafka: joi.object({
                hosts: joi.string().required(),
            }).required(),
            topic: joi.string().required(),
            groupId: joi.string().required(),
            queueProcessor: joi.func(),
            fromOffset: joi.alternatives().try('latest', 'earliest', 'none'),
            autoCommit: joi.boolean().default(false),
            concurrency: joi.number().greater(0).default(CONCURRENCY_DEFAULT),
            fetchMaxBytes: joi.number(),
            createConsumer: joi.boolean().default(true),
        };
        const validConfig = joi.attempt(config, configJoi,
                                        'invalid config params');

        const { zookeeper, kafka, topic, groupId, queueProcessor,
                fromOffset, autoCommit, concurrency, fetchMaxBytes,
                createConsumer } = validConfig;

        this._zookeeperEndpoint = zookeeper && zookeeper.connectionString;
        this._kafkaHosts = kafka.hosts;
        this._fromOffset = fromOffset;
        this._autoCommit = autoCommit;
        this._log = new Logger(CLIENT_ID);
        this._topic = topic;
        this._groupId = groupId;
        this._queueProcessor = queueProcessor;
        this._concurrency = concurrency;
        this._fetchMaxBytes = fetchMaxBytes;
        this._createConsumer = createConsumer;
        this._processingQueue = null;
        this._messagesConsumed = 0;
        this._consumer = null;
        // metrics - consumption
        this._metricsStore = {};
        this._init();
        return this;
    }

    _init() {
        if (this._createConsumer) {
            this._initConsumer();
        }
        async.parallel([
            done => {
                if (this._createConsumer) {
                    return this._consumer.once('connect', done);
                }
                return process.nextTick(done);
            },
        ], () => this.emit('connect'));
    }

    _initConsumer() {
        const consumerParams = {
            'metadata.broker.list': this._kafkaHosts,
            'group.id': this._groupId,
            'enable.auto.commit': this._autoCommit,
            'offset_commit_cb': this._onOffsetCommit.bind(this),
        };
        if (this._fromOffset !== undefined) {
            consumerParams['auto.offset.reset'] = this._fromOffset;
        }
        if (this._fetchMaxBytes !== undefined) {
            consumerParams['fetch.message.max.bytes'] = this._fetchMaxBytes;
        }
        this._consumer = new kafka.KafkaConsumer(consumerParams);
        this._consumer.connect();
        this._consumer.on('ready', () => {
            this.emit('ready');
        });
        return this;
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
        this._consumer.subscribe([this._topic]);

        this._processingQueue = async.queue(
            this._queueProcessor, this._concurrency);
        // when the task queue is empty, commit offset for all
        // consumed partitions and try consuming new messages right away
        this._processingQueue.drain = () => {
            this._consumer.commit();
            this.emit('consumed', this._messagesConsumed);
            this._messagesConsumed = 0;

            this.emit('metrics', this._metricsStore);
            this._metricsStore = {};

            this._tryConsume();
        };

        this._consumer.on('event.error', error => {
            // This is a bit hacky: the "broker transport failure"
            // error occurs when the kafka broker reaps the idle
            // connections every few minutes, and librdkafka handles
            // reconnection automatically anyway, so we ignore those
            // harmless errors (moreover with the current
            // implementation there's no way to access the original
            // error code, so we match the message instead).
            if (!['broker transport failure',
                  'all broker connections are down']
                .includes(error.message)) {
                this._log.error('consumer error', {
                    error,
                    topic: this._topic,
                });
                this.emit('error', error);
            }
        });

        // trigger initial consumption
        this._tryConsume();
        return this;
    }

    _tryConsume() {
        // use non-flowing mode of consumption to add some flow
        // control: explicit consumption of messages is required,
        // needs explicit polling until new messages become available.
        this._consumer.consume(this._concurrency, (err, entries) => {
            if (!err) {
                entries.forEach(entry => {
                    this._messagesConsumed++;
                    this._processingQueue.push(
                        entry, err => this._onEntryProcessingDone(err, entry));
                });
            }
            if (err || entries.length === 0) {
                // retry later to fetch new messages in case no one is
                // available yet in the message queue
                setTimeout(this._tryConsume.bind(this), 1000);
            }
        });
    }

    _onEntryProcessingDone(err, entry) {
        const { topic, partition, offset, key, timestamp } = entry;
        this._log.debug('finished processing of consumed entry', {
            method: 'BackbeatConsumer.subscribe',
            partition,
            offset,
        });
        if (err) {
            this._log.error('error processing an entry', {
                error: err,
                entry: { topic, partition, offset, key, timestamp },
            });
            this.emit('error', err, entry);
        } else if (entry.topic === CRR_TOPIC) {
            const qEntry = QueueEntry.createFromKafkaEntry(entry);
            if (!qEntry.error && qEntry.getReducedLocations) {
                const locations = qEntry.getReducedLocations();
                const bytes = locations.reduce((sum, item) =>
                                               sum + item.size, 0);

                if (!this._metricsStore[qEntry.bucket]) {
                    this._metricsStore[qEntry.bucket] = {
                        ops: 1,
                        bytes,
                    };
                } else {
                    this._metricsStore[qEntry.bucket].ops++;
                    this._metricsStore[qEntry.bucket].bytes += bytes;
                }
            }
        }
    }


    _onOffsetCommit(err, topicPartitions) {
        if (err) {
            // NO_OFFSET is a "soft error" meaning that the same
            // offset is already committed, which occurs because of
            // auto-commit (e.g. if nothing was done by the producer
            // on this partition since last commit).
            if (err === kafka.CODES.ERRORS.ERR__NO_OFFSET) {
                return undefined;
            }
            this._log.error('error committing offset to kafka',
                            { errorCode: err });
            return undefined;
        }
        this._log.debug('commit offsets callback',
                        { topicPartitions });
        return undefined;
    }

    /**
     * get metadata from kafka topics
     * @param {object} params - call params
     * @param {string} params.topic - topic name
     * @param {number} params.timeout - timeout for the request
     * @param {function} cb - callback: cb(err, response)
     * @return {undefined}
     */
    getMetadata(params, cb) {
        this._consumer.getMetadata(params, cb);
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
        const self = this;
        let lastBootstrapId;
        let producer; // eslint-disable-line prefer-const
        let producerTimer; // eslint-disable-line prefer-const
        let consumerTimer; // eslint-disable-line prefer-const
        function consumeCb(err, messages) {
            if (err) {
                return undefined;
            }
            messages.forEach(message => {
                const bootstrapId = JSON.parse(message.value).bootstrapId;
                if (bootstrapId) {
                    self._log.info('bootstraping backbeat consumer: ' +
                                   'received bootstrap message',
                                   { bootstrapId });
                    if (bootstrapId === lastBootstrapId) {
                        self._log.info('backbeat consumer is bootstrapped');
                        clearInterval(producerTimer);
                        clearInterval(consumerTimer);
                        self._consumer.commit();
                        self._consumer.unsubscribe();
                        producer.close(cb);
                    }
                }
            });
            return undefined;
        }
        assert.strictEqual(this._consumer, null);
        producer = new BackbeatProducer({
            kafka: { hosts: 'localhost:9092' },
            topic: this._topic,
        });
        producer.on('ready', () => {
            producerTimer = setInterval(() => {
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
                        this._consumer.on('ready', () => {
                            this._consumer.subscribe([this._topic]);
                            consumerTimer = setInterval(() => {
                                this._consumer.consume(1, consumeCb);
                            }, 2000);
                        });
                    }, 500);
                }
            }, 5000);
        });
    }

    /**
    * force commit the current offset and close the client connection
    * @param {callback} cb - callback to invoke
    * @return {undefined}
    */
    close(cb) {
        if (this._consumer) {
            this._consumer.commit();
            this._consumer.unsubscribe();
            this._consumer.disconnect();
            this._consumer.on('disconnected', () => cb());
        } else {
            process.nextTick(cb);
        }
    }
}

module.exports = BackbeatConsumer;
