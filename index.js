'use strict'; // eslint-disable-line strict

const werelogs = require('werelogs');

const runServer = require('./lib/api/BackbeatServer');

const Config = process.env.TEST_SWITCH !== '1' ? require('./conf/Config') :
    require('./tests/config.json');

const Logger = werelogs.Logger;

werelogs.configure({
    level: Config.log.logLevel,
    dump: Config.log.dumpLevel,
});

runServer(Config, Logger);
