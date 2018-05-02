const { RedisClient } = require('arsenal').metrics;
const config = require('../../../conf/Config');
const werelogs = require('werelogs');

const log = new werelogs.Logger('Backbeat:RedisClient');
werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});

function getRedisClient() {
    return new RedisClient(config.redis, log);
}

module.exports = getRedisClient;
