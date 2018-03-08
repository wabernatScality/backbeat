const { isMasterKey } = require('arsenal/lib/versioning/Version');
const { usersBucket, mpuBucketPrefix } = require('arsenal').constants;

const QueuePopulatorExtension =
          require('../../lib/queuePopulator/QueuePopulatorExtension');
const ObjectQueueEntry = require('./utils/ObjectQueueEntry');

class ReplicationQueuePopulator extends QueuePopulatorExtension {
    constructor(params) {
        super(params);
        this.repConfig = params.config;
    }

    filter(entry) {
        if (entry.key === undefined) {
            // bucket updates have no key in raft log
            return undefined;
        }
        if (entry.bucket === usersBucket) {
            return this._filterBucketOp(entry);
        }
        if (!isMasterKey(entry.key)) {
            return this._filterVersionedKey(entry);
        }
        return undefined;
    }

    _filterBucketOp(entry) {
        if (entry.type !== 'put' ||
            entry.key.startsWith(mpuBucketPrefix)) {
            return;
        }
        this.log.trace('publishing bucket replication entry',
                       { bucket: entry.bucket });
        this.publish(this.repConfig.topic,
                     entry.bucket, JSON.stringify(entry));
    }

    _filterVersionedKey(entry) {
        if (entry.type !== 'put') {
            return;
        }
        const value = JSON.parse(entry.value);
        const queueEntry = new ObjectQueueEntry(entry.bucket,
                                                entry.key, value);
        const sanityCheckRes = queueEntry.checkSanity();
        if (sanityCheckRes) {
            return;
        }
        if (queueEntry.getReplicationStatus() !== 'PENDING') {
            return;
        }
        this.log.trace('publishing object replication entry',
                       { entry: queueEntry.getLogInfo() });
        this.publish(this.repConfig.topic,
                     `${queueEntry.getBucket()}/${queueEntry.getObjectKey()}`,
                     JSON.stringify(entry));

        // Populate metric using site || storageClass (name of custom site)
        const repInfo = value.replicationInfo;
        let site;
        if (repInfo.storageType) {
            // known external cloud (i.e. aws_s3 || azure)
            site = repInfo.storageType;
        } else if (repInfo.storageClass) {
            site = repInfo.storageClass;
        } else {
            site = 'bb-unknown-type';

            this.log.debug('unknown metric type has been processed', {
                method: 'ReplicationQueuePopulator._filterVersionedKey',
                entry: queueEntry.getLogInfo(),
            });
        }

        this._incrementMetrics(site, queueEntry.getBytesMetric());
    }
}

module.exports = ReplicationQueuePopulator;
