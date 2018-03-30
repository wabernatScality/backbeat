/**
 * Get a kafka entry to be stored as a Redis key value failed site.
 * @param {String} bucket - The bucket key name
 * @param {String} key - The object key name
 * @param {String} failedSite - The site which should have the status FAILED
 * @return {Object} The kafka entry
 */
function getKafkaEntry(bucket, key, failedSite) {
    return {
        topic: 'backbeat-replication-status',
        value: `{"bucket":"${bucket}","key":"${key}","value":"{\\"owner-display-name\\":\\"retryuser\\",\\"owner-id\\":\\"1d79449d2b781e585051d057ecf99cad1898120cda80b7b2a94e3fe99d8cad82\\",\\"cache-control\\":\\"\\",\\"content-disposition\\":\\"\\",\\"content-encoding\\":\\"\\",\\"expires\\":\\"\\",\\"content-length\\":1,\\"content-type\\":\\"application/octet-stream\\",\\"content-md5\\":\\"93b885adfe0da089cdf634904fd59f71\\",\\"x-amz-version-id\\":\\"null\\",\\"x-amz-server-version-id\\":\\"\\",\\"x-amz-storage-class\\":\\"STANDARD\\",\\"x-amz-server-side-encryption\\":\\"\\",\\"x-amz-server-side-encryption-aws-kms-key-id\\":\\"\\",\\"x-amz-server-side-encryption-customer-algorithm\\":\\"\\",\\"x-amz-website-redirect-location\\":\\"\\",\\"acl\\":{\\"Canned\\":\\"private\\",\\"FULL_CONTROL\\":[],\\"WRITE_ACP\\":[],\\"READ\\":[],\\"READ_ACP\\":[]},\\"key\\":\\"\\",\\"location\\":[{\\"key\\":\\"3b601427e8bb1f0b96474812fa73db57ac401b1c\\",\\"size\\":1,\\"start\\":0,\\"dataStoreName\\":\\"us-east-1\\",\\"dataStoreETag\\":\\"1:93b885adfe0da089cdf634904fd59f71\\"}],\\"isNull\\":\\"\\",\\"nullVersionId\\":\\"\\",\\"isDeleteMarker\\":false,\\"versionId\\":\\"98477653098936999999RG001  246\\",\\"tags\\":{},\\"replicationInfo\\":{\\"status\\":\\"FAILED\\",\\"backends\\":[{\\"site\\":\\"${failedSite}\\",\\"status\\":\\"FAILED\\",\\"dataStoreVersionId\\":\\"\\"},{\\"site\\":\\"us-east-2\\",\\"status\\":\\"PENDING\\",\\"dataStoreVersionId\\":\\"\\"}],\\"content\\":[\\"DATA\\",\\"METADATA\\"],\\"destination\\":\\"arn:aws:s3:::destination-bucket-1522346900789\\",\\"storageClass\\":\\"${failedSite},us-east-2\\",\\"role\\":\\"arn:aws:iam::604563867484:role/role-for-replication-1522346900789,arn:aws:iam::604563867484:role/role-for-replication-1522346900789\\",\\"storageType\\":\\"aws_s3\\",\\"dataStoreVersionId\\":\\"\\"},\\"dataStoreName\\":\\"us-east-1\\",\\"last-modified\\":\\"2018-03-29T18:08:21.060Z\\",\\"md-model-version\\":3}","site":"${failedSite}"}`, // eslint-disable-line
        offset: 18,
        partition: 0,
        highWaterOffset: 19,
        key,
    };
}

module.exports = getKafkaEntry;
