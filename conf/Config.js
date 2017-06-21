const assert = require('assert');
const fs = require('fs');
const path = require('path');

const ZOOKEEPER_DEFAULT = { host: '127.0.0.1', port: 2181 };
const KAFKA_DEFAULT = { host: '127.0.0.1', port: 9092 };

class Config {
    constructor() {
        /*
         * By default, the config file is "config.json" at the root.
         * It can be overridden using the S3_CONFIG_FILE environment var.
         * By default, the location config file is "locationConfig.json" at
         * the root.
         * It can be overridden using the S3_LOCATION_FILE environment var.
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
        this.kafka = KAFKA_DEFAULT;
        if (config.kafka) {
            assert.strictEqual(typeof config.kafka, 'object', 'config: ' +
                'kafka must be an object');
            assert.strictEqual(typeof config.kafka.host, 'string',
                'config: kafka.host must be a string');
            assert.strictEqual(Number.isInteger(config.kafka.port) &&
                config.kafka.port > 0,
                'config: kafka.port must be a positive integer');
        }

    }
}

module.exports = new Config();
