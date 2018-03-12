'use strict'; // eslint-disable-line
const werelogs = require('werelogs');
const LifecycleProducer = require('./LifecycleProducer');
const { zookeeper, kafka, extensions, s3, auth, transport, log } =
    require('../../../conf/Config');

werelogs.configure({ level: log.logLevel,
                     dump: log.dumpLevel });

const lifecycleProducer =
    new LifecycleProducer(zookeeper, kafka, extensions.lifecycle,
                          s3, auth, transport);

lifecycleProducer.start();
