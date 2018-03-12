'use strict'; // eslint-disable-line

const werelogs = require('werelogs');

const ReplicationStatusProcessor = require('./ReplicationStatusProcessor');

const config = require('../../../conf/Config');
const kafkaConfig = config.kafka;
const repConfig = config.extensions.replication;
const sourceConfig = repConfig.source;

const replicationStatusProcessor =
          new ReplicationStatusProcessor(kafkaConfig, sourceConfig, repConfig);

werelogs.configure({ level: config.log.logLevel,
                     dump: config.log.dumpLevel });

replicationStatusProcessor.start();
