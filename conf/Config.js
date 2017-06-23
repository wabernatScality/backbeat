const assert = require('assert');
const fs = require('fs');
const path = require('path');

const ZOOKEEPER_DEFAULT = { host: '127.0.0.1', port: 2181 };
const KAFKA_DEFAULT = { host: '127.0.0.1', port: 9092 };
const LOGGER_DEFAULT = { logLevel: 'info', dumpLevel: 'error' };

class Config {
    constructor() {
        /*
         * By default, the config file is "config.json" at the root.
         * It can be overridden using the BACKBEAT_CONFIG_FILE environment var.
         */
        this._basePath = path.join(__dirname, '..');
        this.configPath = path.join(__dirname, '../config.json');
        if (process.env.BACKBEAT_CONFIG_FILE !== undefined) {
            this.configPath = process.env.BACKBEAT_CONFIG_FILE;
        }

        let config;
        try {
            const data = fs.readFileSync(this.configPath,
              { encoding: 'utf-8' });
            config = JSON.parse(data);
        } catch (err) {
            throw new Error(`could not parse config file: ${err.message}`);
        }

        // zookeeper
        this.zookeeper = ZOOKEEPER_DEFAULT;
        if (config.zookeeper) {
            assert.strictEqual(typeof config.zookeeper, 'object', 'config: ' +
                'zookeeper must be an object');
            assert.strictEqual(typeof config.zookeeper.host, 'string',
                'config: zookeeper.host must be a string');
            assert.strictEqual(Number.isInteger(config.zookeeper.port) &&
                config.zookeeper.port > 0,
                'config: zookeeper.port must be a positive integer');
            this.zookeeper.host = config.zookeeper.host;
            this.zookeeper.port = config.zookeeper.port;
        }

        // kafka
        this.kafka = KAFKA_DEFAULT;
        if (config.kafka) {
            assert.strictEqual(typeof config.kafka, 'object', 'config: ' +
                'kafka must be an object');
            assert.strictEqual(typeof config.kafka.host, 'string',
                'config: kafka.host must be a string');
            assert.strictEqual(Number.isInteger(config.kafka.port) &&
                config.kafka.port > 0,
                'config: kafka.port must be a positive integer');
            this.kafka.host = config.kafka.host;
            this.kafka.port = config.kafka.port;
        }

        // logger
        this.logger = LOGGER_DEFAULT;
        if (config.log !== undefined) {
            if (config.log.logLevel !== undefined) {
                assert(typeof config.log.logLevel === 'string',
                    'bad config: log.logLevel must be a string');
                this.log.logLevel = config.log.logLevel;
            }
            if (config.log.dumpLevel !== undefined) {
                assert(typeof config.log.dumpLevel === 'string',
                    'bad config: log.dumpLevel must be a string');
                this.log.dumpLevel = config.log.dumpLevel;
            }
            this.logger = config.log;
        }

        // extensions
        this.extensions = {};
        if (config.extensions) {
            assert(typeof config.extensions === 'object', 'bad config: ' +
                'extensions must be an object');
            // supported extensions: replication
            const { replication } = config.extensions;
            if (replication) {
                assert(typeof replication === 'object',
                    'bad config: extensions.replication must be an object');
                const { source, target } = replication;
                this.extensions.replication = { };
                // source config
                assert(typeof source === 'object',
                    'bad config: replication.source must be an object');
                assert(typeof source.host === 'string',
                    'bad config: replication.source.host must be a string');
                assert.strictEqual(Number.isInteger(source.s3Port)
                    && source.s3Port > 0, 'config: ' +
                    'replication.source.s3Port must be a positive integer');
                assert.strictEqual(Number.isInteger(source.vaultPort)
                    && source.vaultPort > 0, 'config: ' +
                    'replication.source.s3Port must be a positive integer');
                this.replication.source = {
                    host: source.host,
                    s3Port: source.s3Port,
                    vaultPort: source.vaultPort,
                };
                // target config
                assert(typeof target === 'object',
                    'bad config: replication.target must be an object');
                assert(typeof target.host === 'string',
                    'bad config: replication.target.host must be a string');
                assert.strictEqual(Number.isInteger(target.s3Port)
                    && target.s3Port > 0, 'config: ' +
                    'replication.target.s3Port must be a positive integer');
                assert.strictEqual(Number.isInteger(target.vaultPort)
                    && target.vaultPort > 0, 'config: ' +
                    'replication.target.s3Port must be a positive integer');
                this.replication.target = {
                    host: target.host,
                    s3Port: target.s3Port,
                    vaultPort: target.vaultPort,
                };

                // target certs
                const { certFilePaths } = replication.target;
                assert(typeof certFilePaths === 'object', 'bad config: ' +
                    'replication.target.certFilePaths must be an object');
                const { key, cert, ca } = certFilePaths;
                const keypath = (key.startsWith('/')) ?
                    key : `${this._basePath}/${key}`;
                const certpath = (cert.startsWith('/')) ?
                    cert : `${this._basePath}/${cert}`;
                let capath = undefined;
                if (ca) {
                    capath = (ca.startsWith('/')) ?
                        ca : `${this._basePath}/${ca}`;
                    assert.doesNotThrow(() =>
                       fs.accessSync(capath, fs.F_OK | fs.R_OK),
                       `File not found or unreachable: ${capath}`);
                }
                assert.doesNotThrow(() =>
                   fs.accessSync(keypath, fs.F_OK | fs.R_OK),
                   `File not found or unreachable: ${keypath}`);
                assert.doesNotThrow(() =>
                   fs.accessSync(certpath, fs.F_OK | fs.R_OK),
                   `File not found or unreachable: ${certpath}`);
                this.replication.target.https = {
                    cert: fs.readFileSync(certpath, 'ascii'),
                    key: fs.readFileSync(keypath, 'ascii'),
                    ca: ca ? fs.readFileSync(capath, 'ascii') : undefined,
                };
            }
        }
    }
}

module.exports = new Config();
