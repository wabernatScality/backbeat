const assert = require('assert');
const http = require('http');
const Redis = require('ioredis');
const { Client, Producer } = require('kafka-node');

const { errors, metrics } = require('arsenal');
const { RedisClient, StatsClient } = metrics;

const StatsModel = require('../../../lib/models/StatsModel');
const config = require('../../config.json');
const { makePOSTRequest, getResponseBody } =
    require('../utils/makePOSTRequest');
const redisConfig = { host: '127.0.0.1', port: 6379 };
const CRR_FAILED_HASH_KEY = 'test:bb:crr:failed';
const CRR_FAILED_DATA =
'{"type":"put","bucket":"queue-populator-test-bucket","key":"hosts\\u000098500086134471999999RG001  0","value":"{\\"md-model-version\\":2,\\"owner-display-name\\":\\"Bart\\",\\"owner-id\\":\\"79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be\\",\\"content-length\\":542,\\"content-type\\":\\"text/plain\\",\\"last-modified\\":\\"2017-07-13T02:44:25.519Z\\",\\"content-md5\\":\\"01064f35c238bd2b785e34508c3d27f4\\",\\"x-amz-version-id\\":\\"null\\",\\"x-amz-server-version-id\\":\\"\\",\\"x-amz-storage-class\\":\\"sf\\",\\"x-amz-server-side-encryption\\":\\"\\",\\"x-amz-server-side-encryption-aws-kms-key-id\\":\\"\\",\\"x-amz-server-side-encryption-customer-algorithm\\":\\"\\",\\"x-amz-website-redirect-location\\":\\"\\",\\"acl\\":{\\"Canned\\":\\"private\\",\\"FULL_CONTROL\\":[],\\"WRITE_ACP\\":[],\\"READ\\":[],\\"READ_ACP\\":[]},\\"key\\":\\"\\",\\"location\\":[{\\"key\\":\\"29258f299ddfd65f6108e6cd7bd2aea9fbe7e9e0\\",\\"size\\":542,\\"start\\":0,\\"dataStoreName\\":\\"file\\",\\"dataStoreETag\\":\\"1:01064f35c238bd2b785e34508c3d27f4\\"}],\\"isDeleteMarker\\":false,\\"tags\\":{},\\"replicationInfo\\":{\\"status\\":\\"PENDING\\",\\"backends\\":[{\\"site\\":\\"sf\\",\\"status\\":\\"PENDING\\",\\"dataStoreVersionId\\":\\"\\"},{\\"site\\":\\"replicationaws\\",\\"status\\":\\"PENDING\\",\\"dataStoreVersionId\\":\\"\\"}],\\"content\\":[\\"DATA\\",\\"METADATA\\"],\\"destination\\":\\"arn:aws:s3:::dummy-dest-bucket\\",\\"storageClass\\":\\"sf\\",\\"role\\":\\"arn:aws:iam::123456789012:role/backbeat\\"},\\"x-amz-meta-s3cmd-attrs\\":\\"uid:0/gname:root/uname:root/gid:0/mode:33188/mtime:1490807629/atime:1499845478/md5:01064f35c238bd2b785e34508c3d27f4/ctime:1490807629\\",\\"versionId\\":\\"98500086134471999999RG001  0\\"}"}'

const fakeLogger = {
    trace: () => {},
    error: () => {},
    info: () => {},
    debug: () => {},
};

const defaultOptions = {
    host: config.server.host,
    port: config.server.port,
    method: 'GET',
};

function getUrl(options, path) {
    return `http://${options.host}:${options.port}${path}`;
}

function setHash(redisClient, keys, value, cb) {
    const cmds = keys.map(key => (['hset', CRR_FAILED_HASH_KEY, key, value]));
    redisClient.batch(cmds, cb);
}

function deleteHash(redisClient, cb) {
    const cmds = ['del', CRR_FAILED_HASH_KEY];
    redisClient.batch([cmds], cb);
}

function makeRetryPOSTRequest(body, cb) {
    const options = Object.assign({}, defaultOptions, {
        method: 'POST',
        path: '/_/crr/failed',
    });
    makePOSTRequest(options, body, cb);
}

function getRequest(path, done) {
    const params = {
        host: '127.0.0.1',
        port: 8900,
        method: 'GET',
        path,
    };
    // eslint-disable-next-line
    const req = http.request(params, res => {
        if (res.statusCode !== 200) {
            return done(res);
        }

        const chunks = [];
        res.on('data', chunk => {
            chunks.push(chunk);
        });
        res.on('end', () => {
            let body;
            try {
                body = JSON.parse(Buffer.concat(chunks).toString());
            } catch (e) {
                return done(e);
            }
            return done(null, body);
        });
    });
    req.on('error', err => done(err));
    req.end();
}

describe('Backbeat Server', () => {
    describe('retry routes', () => {
        const interval = 300;
        const expiry = 900;
        const CRR_FAILED = 'test:bb:crr:failed';

        let redisClient;
        let statsClient;
        let redis;

        before(done => {
            redis = new Redis();
            redisClient = new RedisClient(redisConfig, fakeLogger);
            statsClient = new StatsClient(redisClient, interval, expiry);

            statsClient.reportNewRequest(CRR_FAILED, 1725);
            done();
        });

        after(() => {
            redis.keys('test:bb:crr:failed*').then(keys => {
                const pipeline = redis.pipeline();
                keys.forEach(key => pipeline.del(key));
                return pipeline.exec();
            });
        });
    });

    describe('healthcheck route', () => {
        let data;
        let resCode;
        before(done => {
            const kafkaClient = new Client(config.zookeeper.connectionString,
                'TestClient');
            const testProducer = new Producer(kafkaClient);
            testProducer.createTopics([
                config.extensions.replication.topic,
                config.extensions.replication.replicationStatusTopic,
            ], false, () => {});

            const url = getUrl(defaultOptions, '/_/healthcheck');

            http.get(url, res => {
                resCode = res.statusCode;

                let rawData = '';
                res.on('data', chunk => {
                    rawData += chunk;
                });
                res.on('end', () => {
                    data = JSON.parse(rawData);
                    done();
                });
            });
        });

        it('should get a response with data', done => {
            assert(Object.keys(data).length > 0);
            assert.equal(resCode, 200);
            return done();
        });

        it('should have valid keys', done => {
            const keys = [].concat(...data.map(d => Object.keys(d)));

            assert(keys.includes('metadata'));
            assert(keys.includes('internalConnections'));
            return done();
        });

        it('should be healthy', done => {
            // NOTE: isrHealth is not checked here because circleci
            // kafka will have one ISR only. Maybe isrHealth should
            // be a test for end-to-end
            let internalConnections;
            data.forEach(obj => {
                if (Object.keys(obj).includes('internalConnections')) {
                    internalConnections = obj.internalConnections;
                }
            });
            Object.keys(internalConnections).forEach(key => {
                assert.ok(internalConnections[key]);
            });

            return done();
        });
    });

    describe('metrics routes', () => {
        const interval = 300;
        const expiry = 900;
        const OPS = 'test:bb:ops';
        const BYTES = 'test:bb:bytes';
        const OPS_DONE = 'test:bb:opsdone';
        const BYTES_DONE = 'test:bb:bytesdone';

        let redisClient;
        let statsClient;
        let redis;

        before(done => {
            redis = new Redis();
            redisClient = new RedisClient(redisConfig, fakeLogger);
            statsClient = new StatsModel(redisClient, interval, expiry);

            statsClient.reportNewRequest(OPS, 1725);
            statsClient.reportNewRequest(BYTES, 219800);
            statsClient.reportNewRequest(OPS_DONE, 450);
            statsClient.reportNewRequest(BYTES_DONE, 102700);

            done();
        });

        after(() => {
            redis.keys('*:test:bb:*').then(keys => {
                const pipeline = redis.pipeline();
                keys.forEach(key => {
                    pipeline.del(key);
                });
                return pipeline.exec();
            });
        });

        // TODO: refactor this
        const metricsPaths = [
            '/_/metrics/crr/all',
            '/_/metrics/crr/all/backlog',
            '/_/metrics/crr/all/completions',
            '/_/metrics/crr/all/throughput',
        ];

        metricsPaths.forEach(path => {
            it(`should get a 200 response for route: ${path}`, done => {
                const url = getUrl(defaultOptions, path);

                http.get(url, res => {
                    assert.equal(res.statusCode, 200);
                    done();
                });
            });

            it(`should get correct data keys for route: ${path}`,
            done => {
                getRequest(path, (err, res) => {
                    assert.ifError(err);
                    const key = Object.keys(res)[0];
                    assert(res[key].description);
                    assert.equal(typeof res[key].description, 'string');

                    assert(res[key].results);
                    assert.deepEqual(Object.keys(res[key].results),
                        ['count', 'size']);
                    done();
                });
            });
        });

        const retryPaths = [
            '/_/crr/failed',
            '/_/crr/failed/test-bucket/test-key/test-versionId',
        ];

        retryPaths.forEach(path => {
            it(`should get a 200 response for route: ${path}`, done => {
                const url = getUrl(defaultOptions, path);

                http.get(url, res => {
                    assert.equal(res.statusCode, 200);
                    done();
                });
            });
        });

        // TODO: refactor this
        const allWrongPaths = [
            // general wrong paths
            '/',
            '/metrics/crr/all',
            '/_/metrics',
            '/_/metrics/backlog',
            // wrong category field
            '/_/m/crr/all',
            '/_/metric/crr/all',
            '/_/metric/crr/all/backlog',
            '/_/metricss/crr/all',
            // wrong extension field
            '/_/metrics/c/all',
            '/_/metrics/c/all/backlog',
            '/_/metrics/crrr/all',
            // wrong site field
            // wrong type field
            '/_/metrics/crr/all/backlo',
            '/_/metrics/crr/all/backlogs',
            '/_/metrics/crr/all/completion',
            '/_/metrics/crr/all/completionss',
            '/_/metrics/crr/all/throughpu',
            '/_/metrics/crr/all/throughputs',
        ];

        allWrongPaths.forEach(path => {
            it(`should get a 404 response for route: ${path}`,
            done => {
                const url = getUrl(defaultOptions, path);

                http.get(url, res => {
                    assert.equal(res.statusCode, 404);
                    assert.equal(res.statusMessage, 'Not Found');
                    done();
                });
            });
        });

        it('should return an error for unknown site given', done => {
            getRequest('/_/metrics/crr/wrong-site/completions', err => {
                assert.equal(err.statusCode, 404);
                assert.equal(err.statusMessage, 'Not Found');
                done();
            });
        });

        it('should get a 200 response for route: /_/crr/failed', done => {
            const body = JSON.stringify([{
                bucket: 'bucket',
                key: 'key',
                versionId: 'versionId',
                site: 'site'
            }]);
            makeRetryPOSTRequest(body, (err, res) => {
                assert.ifError(err);
                assert.strictEqual(res.statusCode, 200);
                done();
            });
        });

        const invalidPOSTRequestBodies = [
            'a',
            {},
            [],
            ['a'],
            [{
                // Missing bucket property.
                key: 'b',
                versionId: 'c',
                site: 'd',
            }],
            [{
                // Missing key property.
                bucket: 'a',
                versionId: 'c',
                site: 'd',
            }],
            [{
                // Missing versionId property.
                bucket: 'a',
                key: 'b',
                site: 'd',
            }],
            [{
                // Missing site property.
                bucket: 'a',
                key: 'b',
                versionId: 'c',
            }],
        ]

        invalidPOSTRequestBodies.forEach(body => {
            const invalidBody = JSON.stringify(body);
            it('should get a 400 response for route: /_/crr/failed when ' +
            `given an invalid request body: ${invalidBody}`, done => {
                makeRetryPOSTRequest(invalidBody, (err, res) => {
                    assert.strictEqual(res.statusCode, 400);
                    return getResponseBody(res, (err, resBody) => {
                        assert.ifError(err);
                        const body = JSON.parse(resBody);
                        assert(body.MalformedPOSTRequest);
                        return done();
                    });
                });
            });
        });

        describe('Retry feature with Redis', () => {
            afterEach(done => deleteHash(redisClient, done));

            it('should get correct data for GET route: /_/crr/failed when no ' +
            'hash key has been created', done => {
                getRequest('/_/crr/failed', (err, res) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(res, []);
                    done();
                });
            });

            it('should get correct data for GET route: /_/crr/failed when ' +
            'the hash has been created and there is one hash key', done => {
                const key = 'test-bucket:test-key:test-versionId:test-site';
                setHash(redisClient, key, '{}', err => {
                    assert.ifError(err);
                    getRequest('/_/crr/failed', (err, res) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(res, [{
                            bucket: 'test-bucket',
                            key: 'test-key',
                            versionId: 'test-versionId',
                            site: 'test-site'
                        }]);
                        done();
                    });
                });
            });

            it('should get correct data for GET route: /_/crr/failed when ' +
            'the hash has been created and there are multiple hash keys',
            done => {
                const keys = [
                    'test-bucket:test-key:test-versionId:test-site',
                    'test-bucket-1:test-key-1:test-versionId-1:test-site-1',
                    'test-bucket-2:test-key-2:test-versionId-2:test-site-2',
                ]
                setHash(redisClient, keys, '{}', err => {
                    assert.ifError(err);
                    getRequest('/_/crr/failed', (err, res) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(res, [{
                            bucket: 'test-bucket',
                            key: 'test-key',
                            versionId: 'test-versionId',
                            site: 'test-site'
                        }, {
                            bucket: 'test-bucket-1',
                            key: 'test-key-1',
                            versionId: 'test-versionId-1',
                            site: 'test-site-1'
                        }, {
                            bucket: 'test-bucket-2',
                            key: 'test-key-2',
                            versionId: 'test-versionId-2',
                            site: 'test-site-2'
                        }]);
                        done();
                    });
                });
            });

            it('should get correct data at scale for GET route: /_/crr/failed',
            function (done) {
                this.timeout(15000);
                const cmds = [];
                for (let i = 0; i < 20000; i++) {
                    const keys = [
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-a`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-b`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-c`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-d`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-e`,
                    ];
                    cmds.push(cb => setHash(redisClient, keys, '{}', cb))
                }
                async.parallelLimit(cmds, 10, err => {
                    assert.ifError(err);
                    getRequest('/_/crr/failed', (err, res) => {
                        assert.ifError(err);
                        assert.strictEqual(res.length, 20000 * 5);
                        done();
                    });
                });
            });

            it('should get correct data for GET route: ' +
            '/_/crr/failed/<bucket>/<key>/<versionId> when there is no key', done => {
                getRequest('/_/crr/failed/test-bucket/test-key/test-versionId',
                (err, res) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(res, []);
                    done();
                });
            });

            it('should get correct data for GET route: ' +
            '/_/crr/failed/<bucket>/<key>/<versionId> when there is one hash key',
            done => {
                const keys = [
                    'test-bucket:test-key:test-versionId:test-site',
                    'test-bucket:test-key:test-versionId:test-site-2',
                    'test-bucket-1:test-key-1:test-versionId-1:test-site',
                ]
                setHash(redisClient, keys, '{}', err => {
                    assert.ifError(err);
                    const route = '/_/crr/failed/test-bucket/test-key/test-versionId';
                    return getRequest(route, (err, res) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(res, [{
                            bucket: 'test-bucket',
                            key: 'test-key',
                            versionId: 'test-versionId',
                            site: 'test-site'
                        }, {
                            bucket: 'test-bucket',
                            key: 'test-key',
                            versionId: 'test-versionId',
                            site: 'test-site-2'
                        }]);
                        return done();
                    });
                });
            });

            it('should get correct data for GET route: /_/crr/failed when no ' +
            'hash key has been matched', done => {
                const body = JSON.stringify([{
                    bucket: 'bucket',
                    key: 'key',
                    versionId: 'versionId',
                    site: 'site'
                }]);
                makeRetryPOSTRequest(body, (err, res) => {
                    assert.ifError(err);
                    getResponseBody(res, (err, resBody) => {
                        assert.ifError(err);
                        const body = JSON.parse(resBody);
                        assert.deepStrictEqual(body, []);
                        done();
                    });
                });
            });

            it('should get correct data for POST route: /_/crr/failed ' +
            'there are multiple matching hash keys', done => {
                const keys = [
                    'test-bucket:test-key:test-versionId:test-site-1',
                    'test-bucket:test-key:test-versionId:test-site-2',
                    'test-bucket:test-key:test-versionId:test-site-3',
                ]
                setHash(redisClient, keys, '{}', err => {
                    assert.ifError(err);
                    const body = JSON.stringify([{
                        bucket: 'test-bucket',
                        key: 'test-key',
                        versionId: 'test-versionId',
                        site: 'test-site-1',
                    }, {
                        bucket: 'test-bucket',
                        key: 'test-key',
                        versionId: 'test-versionId',
                        site: 'test-site-unknown', // Should not be in response.
                    }, {
                        bucket: 'test-bucket',
                        key: 'test-key',
                        versionId: 'test-versionId',
                        site: 'test-site-2',
                    }, {
                        bucket: 'test-bucket',
                        key: 'test-key',
                        versionId: 'test-versionId',
                        site: 'test-site-3',
                    }]);
                    makeRetryPOSTRequest(body, (err, res) => {
                        assert.ifError(err);
                        getResponseBody(res, (err, resBody) => {
                            assert.ifError(err);
                            const body = JSON.parse(resBody);
                            assert.deepStrictEqual(body, [{
                                bucket: 'test-bucket',
                                key: 'test-key',
                                versionId: 'test-versionId',
                                site: 'test-site-1',
                                status: 'PENDING',
                            }, {
                                bucket: 'test-bucket',
                                key: 'test-key',
                                versionId: 'test-versionId',
                                site: 'test-site-2',
                                status: 'PENDING',
                            }, {
                                bucket: 'test-bucket',
                                key: 'test-key',
                                versionId: 'test-versionId',
                                site: 'test-site-3',
                                status: 'PENDING',
                            }]);
                            done();
                        });
                    });
                })
            });

            it.only('should get correct data at scale for POST route: /_/crr/failed',
            function (done) {
                this.timeout(15000);
                const cmds = [];
                const reqBody = [];
                for (let i = 0; i < 20000; i++) {
                    reqBody.push({
                        bucket: `bucket-${i}`,
                        key: `key-${i}`,
                        versionId: `versionId-${i}`,
                        site: `site-${i}-a`,
                    }, {
                        bucket: `bucket-${i}`,
                        key: `key-${i}`,
                        versionId: `versionId-${i}`,
                        site: `site-${i}-b`,
                    }, {
                        bucket: `bucket-${i}`,
                        key: `key-${i}`,
                        versionId: `versionId-${i}`,
                        site: `site-${i}-c`,
                    }, {
                        bucket: `bucket-${i}`,
                        key: `key-${i}`,
                        versionId: `versionId-${i}`,
                        site: `site-${i}-d`,
                    }, {
                        bucket: `bucket-${i}`,
                        key: `key-${i}`,
                        versionId: `versionId-${i}`,
                        site: `site-${i}-e`,
                    });
                    const keys = [
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-a`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-b`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-c`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-d`,
                        `bucket-${i}:key-${i}:versionId-${i}:site-${i}-e`,
                    ];
                    cmds.push(cb => setHash(redisClient, keys, '{}', cb))
                }
                const body = JSON.stringify(reqBody);
                async.parallelLimit(cmds, 10, err => {
                    assert.ifError(err);
                    makeRetryPOSTRequest(body, (err, res) => {
                        assert.ifError(err);
                        getResponseBody(res, (err, resBody) => {
                            assert.ifError(err);
                            const body = JSON.parse(resBody);
                            console.log('RES BODY', body)
                            assert.strictEqual(body.length, 20000 * 5);
                            done();
                        });
                    });
                });
            });

        });

        it('should return an error for unknown site given', done => {
            getRequest('/_/metrics/crr/wrong-site/completions', err => {
                assert.equal(err.statusCode, 404);
                assert.equal(err.statusMessage, 'Not Found');
                done();
            });
        });

        it('should get the right data for route: ' +
        '/_/metrics/crr/all/backlog', done => {
            getRequest('/_/metrics/crr/all/backlog', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                // Backlog count = OPS - OPS_DONE
                assert.equal(res[key].results.count, 1275);
                // Backlog size = (BYTES - BYTES_DONE) / 1000
                assert.equal(res[key].results.size, 117.1);
                done();
            });
        });

        it('should get the right data for route: ' +
        '/_/metrics/crr/all/completions', done => {
            getRequest('/_/metrics/crr/all/completions', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                // Completions count = OPS_DONE
                assert.equal(res[key].results.count, 450);
                // Completions bytes = BYTES_DONE / 1000
                assert.equal(res[key].results.size, 102.7);
                done();
            });
        });

        it('should get the right data for route: ' +
        '/_/metrics/crr/all/throughput', done => {
            getRequest('/_/metrics/crr/all/throughput', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                // Throughput count = OPS_DONE / EXPIRY
                assert.equal(res[key].results.count, 0.5);
                // Throughput bytes = (BYTES_DONE / 1000) / EXPIRY
                assert.equal(res[key].results.size, 0.11);
                done();
            });
        });

        it('should return all metrics for route: ' +
        '/_/metrics/crr/all', done => {
            getRequest('/_/metrics/crr/all', (err, res) => {
                assert.ifError(err);
                const keys = Object.keys(res);
                assert(keys.includes('backlog'));
                assert(keys.includes('completions'));
                assert(keys.includes('throughput'));

                assert(res.backlog.description);
                // Backlog count = OPS - OPS_DONE
                assert.equal(res.backlog.results.count, 1275);
                // Backlog size = (BYTES - BYTES_DONE) / 1000
                assert.equal(res.backlog.results.size, 117.1);

                assert(res.completions.description);
                // Completions count = OPS_DONE
                assert.equal(res.completions.results.count, 450);
                // Completions bytes = BYTES_DONE / 1000
                assert.equal(res.completions.results.size, 102.7);

                assert(res.throughput.description);
                // Throughput count = OPS_DONE / EXPIRY
                assert.equal(res.throughput.results.count, 0.5);
                // Throughput bytes = (BYTES_DONE / 1000) / EXPIRY
                assert.equal(res.throughput.results.size, 0.11);

                done();
            });
        });
    });

    it('should get a 404 route not found error response', () => {
        const url = getUrl(defaultOptions, '/_/invalidpath');

        http.get(url, res => {
            assert.equal(res.statusCode, 404);
        });
    });

    it('should get a 405 method not allowed from invalid http verb', done => {
        const options = Object.assign({}, defaultOptions);
        options.method = 'POST';
        options.path = '/_/healthcheck';

        const req = http.request(options, res => {
            assert.equal(res.statusCode, 405);
        });
        req.on('error', err => {
            assert.ifError(err);
        });
        req.end();
        done();
    });
});
