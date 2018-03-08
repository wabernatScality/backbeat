const assert = require('assert');
const http = require('http');
const Redis = require('ioredis');
const { Client, Producer } = require('kafka-node');

const { RedisClient, StatsClient } = require('arsenal').metrics;

const config = require('../../config.json');
const allRoutes = require('../../../lib/api/routes');
const redisConfig = { host: '127.0.0.1', port: 6379 };

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
    before(done => {
        const kafkaClient = new Client(config.zookeeper.connectionString,
            'TestClient');
        const testProducer = new Producer(kafkaClient);
        testProducer.createTopics([
            config.extensions.replication.topic,
            config.extensions.replication.replicationStatusTopic,
        ], false, () => {});
        done();
    });

    describe('healthcheck route', () => {
        let data;
        let resCode;
        before(done => {
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
        const interval = 100;
        const expiry = 300;
        const OPS = 'test:bb:ops';
        const BYTES = 'test:bb:bytes';
        const OPS_DONE = 'test:bb:opsdone';
        const BYTES_DONE = 'test:bb:bytesdone';

        const destconfig = config.extensions.replication.destination;
        const site1 = destconfig.bootstrapList[0].site;
        const site2 = destconfig.bootstrapList[1].site;

        let redisClient;
        let statsClient;
        let redis;

        before(done => {
            redis = new Redis();
            redisClient = new RedisClient(redisConfig, fakeLogger);
            statsClient = new StatsClient(redisClient, interval, expiry);

            statsClient.reportNewRequest(`${OPS}:${site1}`, 1725);
            statsClient.reportNewRequest(`${BYTES}:${site1}`, 219800);
            statsClient.reportNewRequest(`${OPS_DONE}:${site1}`, 450);
            statsClient.reportNewRequest(`${BYTES_DONE}:${site1}`, 102700);

            statsClient.reportNewRequest(`${OPS}:${site2}`, 900);
            statsClient.reportNewRequest(`${BYTES}:${site2}`, 294300);
            statsClient.reportNewRequest(`${OPS_DONE}:${site2}`, 300);
            statsClient.reportNewRequest(`${BYTES_DONE}:${site2}`, 187400);

            done();
        });

        after(() => {
            redis.keys('test:bb:*').then(keys => {
                const pipeline = redis.pipeline();
                keys.forEach(key => {
                    pipeline.del(key);
                });
                return pipeline.exec();
            });
        });

        allRoutes.forEach(route => {
            if (route.path.indexOf('healthcheck') === -1) {
                it(`should get a 200 response for route: ${route.path}`,
                done => {
                    const url = getUrl(defaultOptions, route.path);

                    http.get(url, res => {
                        assert.equal(res.statusCode, 200);
                        done();
                    });
                });

                it(`should get correct data keys for route: ${route.path}`,
                done => {
                    getRequest(route.path, (err, res) => {
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
            }
        });

        it('should return an error for unknown site given', done => {
            getRequest('/_/metrics/backlog/wrong-site', err => {
                assert.equal(err.statusCode, 404);
                assert.equal(err.statusMessage, 'Not Found');
                done();
            });
        });

        it(`should get the right data for route: /_/metrics/backlog/${site1}`,
        done => {
            getRequest(`/_/metrics/backlog/${site1}`, (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                assert.equal(res[key].results.count, 1275);
                assert.equal(res[key].results.size, 117.1);
                done();
            });
        });

        it('should get the right data for route: /_/metrics/backlog', done => {
            getRequest('/_/metrics/backlog', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                assert.equal(res[key].results.count, 1875);
                assert.equal(res[key].results.size, 224);
                done();
            });
        });

        it('should get the right data for route: /_/metrics/completions',
        done => {
            getRequest('/_/metrics/completions', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                assert.equal(res[key].results.count, 750);
                assert.equal(res[key].results.size, 290.1);
                done();
            });
        });

        it('should get the right data for route: ' +
        `/_/metrics/completions/${site1}`, done => {
            getRequest(`/_/metrics/completions/${site1}`, (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                assert.equal(res[key].results.count, 450);
                assert.equal(res[key].results.size, 102.7);
                done();
            });
        });

        it('should get the right data for route: /_/metrics/throughput',
        done => {
            getRequest('/_/metrics/throughput', (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                assert.equal(res[key].results.count, 0.83);
                assert.equal(res[key].results.size, 0.32);
                done();
            });
        });

        it('should get the right data for route: ' +
        `/_/metrics/throughput/${site1}`, done => {
            getRequest(`/_/metrics/throughput/${site1}`, (err, res) => {
                assert.ifError(err);
                const key = Object.keys(res)[0];
                assert.equal(res[key].results.count, 0.5);
                assert.equal(res[key].results.size, 0.11);
                done();
            });
        });

        it('should return all metrics for route: /_/metrics', done => {
            getRequest('/_/metrics', (err, res) => {
                assert.ifError(err);
                const keys = Object.keys(res);
                assert(keys.includes('backlog'));
                assert(keys.includes('completions'));
                assert(keys.includes('throughput'));

                assert(res.backlog.description);
                assert.equal(res.backlog.results.count, 1875);
                assert.equal(res.backlog.results.size, 224);

                assert(res.completions.description);
                assert.equal(res.completions.results.count, 750);
                assert.equal(res.completions.results.size, 290.1);

                assert(res.throughput.description);
                assert.equal(res.throughput.results.count, 0.83);
                assert.equal(res.throughput.results.size, 0.32);

                done();
            });
        });

        it(`should return all metrics for route: /_/metrics/${site1}`,
        done => {
            getRequest(`/_/metrics/${site1}`, (err, res) => {
                assert.ifError(err);
                const keys = Object.keys(res);
                assert(keys.includes('backlog'));
                assert(keys.includes('completions'));
                assert(keys.includes('throughput'));

                assert(res.backlog.description);
                assert.equal(res.backlog.results.count, 1275);
                assert.equal(res.backlog.results.size, 117.1);

                assert(res.completions.description);
                assert.equal(res.completions.results.count, 450);
                assert.equal(res.completions.results.size, 102.7);

                assert(res.throughput.description);
                assert.equal(res.throughput.results.count, 0.5);
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