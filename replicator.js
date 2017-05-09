const arsenal = require('arsenal');
const Logger = require('werelogs').Logger;
const Publisher = require('./lib/Publisher');
const MetadataFileClient = arsenal.storage.metadata.client;
const logger = new Logger('Backbeat:Replication');
const log = logger.newRequestLogger();

const sourceMD = new MetadataFileClient({
    metadataHost: 'localhost',
    metadataPort: '9990',
    recordLogEnabled: true,
    log: {
        logLevel: 'info',
        dumpLevel: 'error',
    },
});

const pub = new Publisher({
    zookeeper: { host: 'localhost', port: 2181 },
    log: { logLevel: 'info', dumpLevel: 'error' },
    topic: 'replication',
    partition: 1,
});

const BUCKET = 'demo';
let SEQUENCENUM = 0;
sourceMD.openRecordLog(BUCKET, (err, logProxy) => {
    if (err) {
        return log.error('error fetching log stream', { error: err });
    }
    pub.setClient(err => {
        if (err) {
            return log.error(err);
        }
        const readOptions = { minSeq: (SEQUENCENUM + 1) };
        logProxy.readRecords(readOptions, (err, recordStream) => {
            recordStream.on('data', record => {
                const value = JSON.parse(record.value);
                const repStatus = value['x-amz-replication-status'];
                if (record.type === 'put' && record.key.includes('\u0000')
                && repStatus === 'PENDING') {
                    pub.publish(JSON.stringify(record), err => {
                        if (err) {
                            return log.error('error publishing entries');
                        }
                        SEQUENCENUM = record.seq;
                        return log.info('entries published successfully');
                    });
                }
            });
            recordStream.on('end', () => {
		log.info('ending record stream');
//                pub.close();
            });
        });
        return undefined;
    });
    return undefined;
});
