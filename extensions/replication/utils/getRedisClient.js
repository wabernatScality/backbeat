const { RedisClient } = require('arsenal').metrics;
const config = require('../../../conf/Config');

function getRedisClient(log) {
    return new RedisClient(config.redis, log);
}

module.exports = getRedisClient;
