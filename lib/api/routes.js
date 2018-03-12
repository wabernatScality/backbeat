/*
    This file contains all unique details about API routes exposed by Backbeats
    API server.
*/

const config = process.env.TEST_SWITCH !== '1' ? require('../../conf/Config') :
    require('../../tests/config.json');
const redisKeys = require('../../extensions/replication/constants').redisKeys;

// TODO: currently only getting sites for replication
const bootstrapList = config.extensions.replication.destination.bootstrapList;
const validCRRSites = bootstrapList.map(s => (s.type ? s.type : s.site));

module.exports = [
    {
        category: 'healthcheck',
        type: 'basic',
        method: 'getHealthcheck',
    },
    {
        category: 'metrics',
        type: 'backlog',
        extensions: { crr: [...validCRRSites, 'all'] },
        method: 'getBacklog',
        dataPoints: [redisKeys.ops, redisKeys.opsDone, redisKeys.bytes,
            redisKeys.bytesDone],
    },
    {
        category: 'metrics',
        type: 'completions',
        extensions: { crr: [...validCRRSites, 'all'] },
        method: 'getCompletions',
        dataPoints: [redisKeys.opsDone, redisKeys.bytesDone],
    },
    {
        category: 'metrics',
        type: 'throughput',
        extensions: { crr: [...validCRRSites, 'all'] },
        method: 'getThroughput',
        dataPoints: [redisKeys.opsDone, redisKeys.bytesDone],
    },
    {
        category: 'metrics',
        type: 'all',
        extensions: { crr: [...validCRRSites, 'all'] },
        method: 'getAllMetrics',
        dataPoints: [redisKeys.ops, redisKeys.opsDone, redisKeys.bytes,
            redisKeys.bytesDone],
    },
];
