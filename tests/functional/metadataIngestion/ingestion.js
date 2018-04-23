const async = require('async');
const http = require('http');
const kafka = require('node-rdkafka');
const zookeeper = require('../../../lib/clients/zookeeper');

const QueuePopulator = require('../../../lib/queuePopulator/QueuePopulator');
const IngestionProducer =
    require('../../../lib/queuePopulator/IngestionProducer');
const MetadataMock = require('../../utils/MetadataMock');
const testConfig = require('./config.json');

const testKafkaConfig = {
    'metadata.broker.list': 'localhost:9092',
    'group.id': 'testid',
};

const testZkPaths = [
    '/backbeat',
    '/backbeat/ingestion',
    '/backbeat/ingestion/source1',
    '/backbeat/ingestion/source1/raft-id-dispatcher',
    '/backbeat/ingestion/source1/raft-id-dispatcher/leaders',
    '/backbeat/ingestion/source1/raft-id-dispatcher/owners',
    '/backbeat/ingestion/source1/raft-id-dispatcher/provisions/1',
    '/backbeat/ingestion/source1/raft-id-dispatcher/provisions/2',
    '/backbeat/ingestion/source1/raft-id-dispatcher/provisions/3',
    '/backbeat/ingestion/source1/raft-id-dispatcher/provisions/4',
    '/backbeat/ingestion/source1/raft-id-dispatcher/provisions/5',
    '/backbeat/ingestion/source1/raft-id-dispatcher/provisions/6',
    '/backbeat/ingestion/source1/raft-id-dispatcher/provisions/7',
    '/backbeat/ingestion/source1/raft-id-dispatcher/provisions/8',
    '/queue-populator',
    '/queue-populator/logState',
    '/queue-populator/logState/raft_1',
    '/queue-populator/logState/raft_2',
    '/queue-populator/logState/raft_3',
    '/queue-populator/logState/raft_4',
    '/queue-populator/logState/raft_5',
    '/queue-populator/logState/raft_6',
    '/queue-populator/logState/raft_7',
    '/queue-populator/logState/raft_8',
];

const logOffsetPaths = [
    { path: '/queue-populator/logState/raft_1/logOffset', value: '1' },
    { path: '/queue-populator/logState/raft_2/logOffset', value: '1' },
    { path: '/queue-populator/logState/raft_3/logOffset', value: '1' },
    { path: '/queue-populator/logState/raft_4/logOffset', value: '1' },
    { path: '/queue-populator/logState/raft_5/logOffset', value: '1' },
    { path: '/queue-populator/logState/raft_6/logOffset', value: '1' },
    { path: '/queue-populator/logState/raft_7/logOffset', value: '1' },
    { path: '/queue-populator/logState/raft_8/logOffset', value: '1' },
];

describe.only('Ingest metadata to kafka', () => {
    let metadataMock;
    let httpServer;
    let kafkaConsumer;
    let queuePopulator;
    let zkClient;

    before(function before(done) {
        async.waterfall([
            next => {
                kafkaConsumer = new kafka.KafkaConsumer(testKafkaConfig);
                return next();
            },
            next => {
                kafkaConsumer.connect({ allTopics: true }, (err, res) => {
                    console.log('attempting to connect to kafkaConsumer');
                    console.log(err, res);
                    return next(err);
                });
            },
            next => {
                metadataMock = new MetadataMock();
                httpServer = http.createServer((req, res) =>
                    metadataMock.onRequest(req, res)).listen(7779);
                return next();
            },
            // next => {
            //     iProducer = new IngestionProducer({
            //         host: 'localhost:7779',
            //         port: 7779,
            //     });
            //     return next();
            // },
            next => {
                zkClient = zookeeper.createClient('127.0.0.1:2181');
                zkClient.connect();
                zkClient.once('error', (err, res) => {
                    console.log('error connecting to zookeeper');
                    console.log(err, res);
                    throw err;
                });
                zkClient.once('ready', (err, res) => {
                    console.log('zkclient is ready');
                    console.log(err, res);
                    return next();
                });
            },
            next => {
                return async.each(testZkPaths, (path, cb) => {
                    return zkClient.mkdirp(path, (err, res) => {
                        console.log('trying to mkdirp', err, res);
                        return cb(err, res);
                    });
                }, next);
            },
            next => {
                return async.each(logOffsetPaths, (logOffset, cb) => {
                    const buf = Buffer.from(logOffset.value, 'utf-8');
                    console.log('here is the buffer', buf);
                    return async.waterfall([
                        next => zkClient.mkdirp(logOffset.path, buf, (err, res) => {
                            console.log('trying to create logOffset', err, res);
                            return next();
                        }),
                        next => zkClient.setData(logOffset.path, buf, (err, res) => {
                            console.log('trying to set data to logOffset', err, res);
                            return next();
                        }),
                    ], cb);
                }, next);
            },
            next => {
                return zkClient.getData(logOffsetPaths[0].path, (err, data) => {
                    console.log(`DATA AT ${logOffsetPaths[0]}.path`, err, data);
                    return next();
                });
            },
            next => {
                queuePopulator = new QueuePopulator(testConfig.zookeeper,
                testConfig.kafka, testConfig.queuePopulator, testConfig.metrics,
                testConfig.redis, testConfig.extensions, testConfig.ingestion);
                // return queuePopulator.open((err, res) => {
                //     console.log('opening queue populator', err, res);
                //     return next();
                // });
                queuePopulator.open(() => {});
                queuePopulator.on('logReady', (err, res) => {
                    console.log('LOG IS READY');
                    return next();
                });
            },
        ], done);
    });

    after(done => {
        httpServer.close();
        done();
    });

    it('should store metadata ingested from remote cloud backend', done => {
        return async.waterfall([
            // next => {
            //     console.log('this.iProducer', iProducer);
            //     next();
            // },
            // next => iProducer.snapshot(1, (err, res) => {
            //     console.log('WE PRODUCED SNAPSHOT', res);
            //     return next();
            // }),
            next => {
                console.log('WE WILL PROCESS ALL LOG ENTRIES NOW');
                queuePopulator.processAllLogEntries({ maxRead: 10 },
                (err, counters) => {
                    console.log('attempting to process all log entries');
                    console.log(err, counters);
                    return next();
                });
            },
            next => {
                return kafkaConsumer.getMetadata({}, (err, res) => {
                    console.log('Getting metadata from kafkaConsumer', err);
                    console.log('Getting metadata from kafkaConsumer', res);
                    return next();
                });
            }
        ], () => {
            console.log('finishing');
            return done();
        });
    });
});
