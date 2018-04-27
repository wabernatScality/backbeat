const assert = require('assert');
const async = require('async');
const http = require('http');
const kafka = require('node-rdkafka');
const zookeeper = require('../../../lib/clients/zookeeper');

const QueuePopulator = require('../../../lib/queuePopulator/QueuePopulator');
const MetadataMock = require('../../utils/MetadataMock');
const testConfig = require('./config.json');

const testKafkaConfig = {
    'metadata.broker.list': 'localhost:9092',
    'group.id': 'testid',
    'enable.auto.commit': true,
};

const testZkPaths = [
    '/backbeat',
    '/backbeat/ingestion',
    '/backbeat/ingestion/source1',
    '/backbeat/ingestion/source1/raft-id-dispatcher',
    '/backbeat/ingestion/source1/raft-id-dispatcher/leaders',
    '/backbeat/ingestion/source1/raft-id-dispatcher/owners',
    '/backbeat/ingestion/source1/raft-id-dispatcher/provisions/1',
    '/queue-populator',
    '/queue-populator/logState',
    '/queue-populator/logState/raft_1',
];

const logOffsetPaths = [
    { path: '/queue-populator/logState/raft_1/logOffset', value: '1' },
];
// 
// const expectedLogs = [
//     { type: 'put', bucket: 'users..bucket', key: 'bucket1', value: null },
//     { type: 'put', bucket: 'users..bucket', key: 'bucket2', value: null },
//     {
//         type: 'put',
//         bucket: 'xxxfriday10',
//         key: 'xxxfriday10',
//         value: {
//             acl: {
//                 Canned: 'private',
//                 FULL_CONTROL: [],
//                 WRITE: [],
//                 WRITE_ACP: [],
//                 READ: [],
//                 READ_ACP: [],
//             },
//             name: 'xxxfriday10',
//             owner:
//             '94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8',
//             ownerDisplayName: 'test_1518720219',
//             creationDate: '2018-02-16T21:55:16.415Z',
//             mdBucketModelVersion: 5,
//             transient: false,
//             deleted: false,
//             serverSideEncryption: null,
//             versioningConfiguration: null,
//             locationConstraint: null,
//             cors: null,
//             replicationConfiguration: null,
//             lifecycleConfiguration: null,
//         },
//     },
//     {
//         type: 'put',
//         bucket: 'xxxfriday10',
//         key: 'xxxfriday10',
//         value: {
//             acl: {
//                 Canned: 'private',
//                 FULL_CONTROL: [],
//                 WRITE: [],
//                 WRITE_ACP: [],
//                 READ: [],
//                 READ_ACP: [],
//             },
//             name: 'xxxfriday10',
//             owner:
//             '94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8',
//             ownerDisplayName: 'test_1518720219',
//             creationDate: '2018-02-16T21:55:16.415Z',
//             mdBucketModelVersion: 5,
//             transient: false,
//             deleted: false,
//             serverSideEncryption: null,
//             versioningConfiguration: null,
//             locationConstraint: null,
//             cors: null,
//             replicationConfiguration: null,
//             lifecycleConfiguration: null,
//         },
//     },
//     { type: 'put', bucket: 'bucket1', key: 'testobject1',
//     value: { metadata: 'dogsAreGood' } },
//     { type: 'put', bucket: 'bucket2', key: 'testobject1',
//     value: { metadata: 'dogsAreGood' } },
// ];

const expectedLogs = [
    '{"type":"put","bucket":"users..bucket","key":"bucket1","value":null}',
    '{"type":"put","bucket":"users..bucket","key":"bucket2","value":null}',
    '{"type":"put","bucket":"xxxfriday10","key":"xxxfriday10","value":' +
    '"{\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE\":[],' +
    '\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"name\":\"xxxfriday10\",' +
    '\"owner\":\"94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb4' +
    '60f2f84d8\",\"ownerDisplayName\":\"test_1518720219\",\"creationDate\":' +
    '\"2018-02-16T21:55:16.415Z\",\"mdBucketModelVersion\":5,' +
    '\"transient\":false,\"deleted\":false,\"serverSideEncryption\":null,' +
    '\"versioningConfiguration\":null,\"locationConstraint\":null,' +
    '\"cors\":null,\"replicationConfiguration\":null,' +
    '\"lifecycleConfiguration\":null}"}',
    '{"type":"put","bucket":"xxxfriday10","key":"xxxfriday10","value":' +
    '"{\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE\":[],' +
    '\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"name\":\"xxxfriday10\",' +
    '\"owner\":\"94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb4' +
    '60f2f84d8\",\"ownerDisplayName\":\"test_1518720219\",\"creationDate\":' +
    '\"2018-02-16T21:55:16.415Z\",\"mdBucketModelVersion\":5,' +
    '\"transient\":false,\"deleted\":false,\"serverSideEncryption\":null,' +
    '\"versioningConfiguration\":null,\"locationConstraint\":null,' +
    '\"cors\":null,\"replicationConfiguration\":null,' +
    '\"lifecycleConfiguration\":null}"}',
    '{"type":"put","bucket":"bucket1","key":"testobject1","value":' +
    '"{\"metadata\":\"dogsAreGood\"}"}',
    '{"type":"put","bucket":"bucket2","key":"testobject1","value":' +
    '"{\"metadata\":\"dogsAreGood\"}"}',
];

describe.only('Ingest metadata to kafka', () => {
    let metadataMock;
    let httpServerSnapshot;
    let httpServerLogs;
    let queuePopulator;
    let zkClient;

    before(done => {
        async.waterfall([
            next => {
                metadataMock = new MetadataMock();
                httpServerSnapshot = http.createServer((req, res) =>
                    metadataMock.onRequest(req, res)).listen(7779);
                httpServerLogs = http.createServer((req, res) =>
                    metadataMock.onRequest(req, res)).listen(9000);
                return next();
            },
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
                        fin => zkClient.mkdirp(logOffset.path, buf, (err, res) => {
                            console.log('trying to create logOffset', err, res);
                            return fin();
                        }),
                        fin => zkClient.setData(logOffset.path, buf, (err, res) => {
                            console.log('trying to set data to logOffset', err, res);
                            return fin();
                        }),
                    ], cb);
                }, next);
            },
            next => {
                queuePopulator = new QueuePopulator(testConfig.zookeeper,
                testConfig.kafka, testConfig.queuePopulator, testConfig.metrics,
                testConfig.redis, testConfig.extensions, testConfig.ingestion);
                queuePopulator.open(() => {});
                queuePopulator.on('logReady', () => {
                    console.log('LOG IS READY');
                    return next();
                });
            },
        ], done);
    });

    after(done => {
        httpServerSnapshot.close();
        httpServerLogs.close();
        done();
    });

    it('should store metadata ingested from remote cloud backend', done => {
        async.parallel([
            function processLog(next) {
                console.log('WE WILL PROCESS ALL LOG ENTRIES NOW');
                return queuePopulator.processAllLogEntries({ maxRead: 10 },
                (err, counters) => {
                    console.log('attempting to process all log entries');
                    console.log(err, counters);
                    return next();
                });
            },
            function readstream(next) {
                let expectedKafkaLogs = expectedLogs;
                const stream = kafka.KafkaConsumer.createReadStream(testKafkaConfig, {}, {
                    topics: 'backbeat-ingestion',
                });
                
                stream.on('error', function (err) {
                    console.log('ERR', err);
                });
                
                stream.on('data', data => {
                    console.log('index of the string, we have to have something', expectedKafkaLogs.indexOf(data.value.toString()) > -1);
                    assert(expectedKafkaLogs.indexOf(data.value.toString()) > -1);
                    console.log('data from stream', data);
                    console.log('data from stream, value', data.value.toString());
                    console.log('data from stream, value json parse', JSON.parse(data.value.toString()));
                    console.log('data from stream to string');
                });
                stream.on('end', () => {
                    return next();
                });
            },
        ], () => {
            console.log('finishing');
            return done();
        });
    });
});
