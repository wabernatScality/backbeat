const async = require('async');
const commander = require('commander');
const { S3, IAM, SharedIniFileCredentials } = require('aws-sdk');

const { Logger } = require('werelogs');
const config = require('../conf/Config');

// TRUST POLICY
const trustPolicy = {
    Version: '2012-10-17',
    Statement: [
        {
            Effect: 'Allow',
            Principal: {
                Service: 'backbeat', /* "s3.amazonaws.com" */
            },
            Action: 'sts:AssumeRole',
        },
    ],
};

// RESOURCE POLICY
function _buildResourcePolicy(source, target) {
    return {
        Version: '2012-10-17',
        Statement: [
            {
                Effect: 'Allow',
                Action: [
                    's3:GetObjectVersion',
                    's3:GetObjectVersionAcl',
                ],
                Resource: [
                    `arn:aws:s3:::${source}/*`,
                ],
            },
            {
                Effect: 'Allow',
                Action: [
                    's3:ListBucket',
                    's3:GetReplicationConfiguration',
                ],
                Resource: [
                    `arn:aws:s3:::${source}`,
                ],
            },
            {
                Effect: 'Allow',
                Action: [
                    's3:ReplicateObject',
                    's3:ReplicateDelete',
                ],
                Resource: `arn:aws:s3:::${target}/*`,
            },
        ],
    };
}

function _setupS3Client(host, port, profile) {
    const credentials = new SharedIniFileCredentials({ profile });
    return new S3({
        endpoint: `http://${host}:${port}`,
        sslEnabled: false,
        credentials,
        s3ForcePathStyle: true,
    });
}


function _setupIAMClient(host, port, profile) {
    const credentials = new SharedIniFileCredentials({ profile });
    return new IAM({
        endpoint: `http://${host}:${port}`,
        sslEnabled: false,
        credentials,
        s3ForcePathStyle: true,
    });
}

class _SetupReplication {

    constructor(sourceBucket, targetBucket, config) {
        const { source, destination } = config.extensions.replication;
        this._sourceBucket = sourceBucket;
        this._targetBucket = targetBucket;
        this._s3 = {
            source: _setupS3Client(source.s3.host, source.s3.port,
                'backbeatsource'),
            target: _setupS3Client(destination.s3.host, destination.s3.port,
                'backbeattarget'),
        };
        this._iam = {
            source: _setupIAMClient(source.iam.host, source.iam.port,
                'backbeatsource'),
            target: _setupIAMClient(destination.iam.host, destination.iam.port,
                'backbeattarget'),
        };
    }

    _createBucket(where, cb) {
        const bucket = where === 'source' ? this._sourceBucket :
            this._targetBucket;
        this._s3[where].createBucket({ Bucket: bucket }, cb);
    }

    _createRole(where, cb) {
        const params = {
            AssumeRolePolicyDocument: encodeURIComponent(
                JSON.stringify(trustPolicy)),
            RoleName: `bb-replication-${Date.now()}`,
            Path: '/',
        };
        this._iam[where].createRole(params, cb);
    }

    _createPolicy(where, cb) {
        const params = {
            PolicyDocument: JSON.stringify(
                _buildResourcePolicy(this._sourceBucket, this._targetBucket)),
            PolicyName: `bb-replication-${Date.now()}`,
        };
        this._iam[where].createPolicy(params, cb);
    }

    _enableVersioning(where, cb) {
        const bucket = where === 'source' ? this._sourceBucket :
            this._targetBucket;
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        };
        this._s3[where].putBucketVersioning(params, cb);
    }

    _attachResourcePolicy(policyArn, roleName, where, cb) {
        const params = {
            PolicyArn: policyArn,
            RoleName: roleName,
        };
        this._iam[where].attachRolePolicy(params, cb);
    }

    _enableReplication(roleArns, cb) {
        const params = {
            Bucket: this._sourceBucket,
            ReplicationConfiguration: {
                Role: roleArns,
                Rules: [{
                    Destination: {
                        Bucket: `arn:aws:s3:::${this._targetBucket}`,
                    },
                    Prefix: '',
                    Status: 'Enabled',
                }],
            },
        };

        this.s3.source.putBucketReplication(params, cb);
    }

    _parallelTasks(cb) {
        async.parallel({
            sourceBucket: this._createBucket('source', cb),
            targetBucket: this._createBucket('target', cb),
            sourceRole: this._createRole('source', cb),
            targetRole: this._createRole('target', cb),
            sourcePolicy: this._createPolicy('source', cb),
            targetPolicy: this._createPolicy('target', cb),
        }, cb);
    }

    _seriesTasks(data, cb) {
        const roleArns = `${data.sourceRole.arn},${data.targetRole.arn}`;
        async.series([
            next => this._enableVersioning('source', next),
            next => this._enableVersioning('target', next),
            next => this._attachResourcePolicy(data.sourcePolicy.arn,
                data.sourceRole.arn, 'source', next),
            next => this._attachResourcePolicy(data.targetPolicy.arn,
                data.targetPolicy.arn, 'target', next),
            next => this._enableReplication(roleArns, next),
        ], cb);
    }

    run(cb) {
        async.waterfall([
            next => this._parallelTasks(next),
            (setupInfo, next) => this._seriesTasks(setupInfo, next),
        ], cb);
    }
}

// const sourceCredentials = new SharedIniFileCredentials({
//     profile: 'backbeatsource',
// });
// const destinationCredentials = new SharedIniFileCredentials({
//     profile: 'backbeatdestination',
// });


// NOTE: Use commander to initiate action of the script
// Wrap the async and setup calls into a function
commander
  .version('0.1.0')
  // .usage('<source> <destination>')
  .arguments('<source> <destination>')
  .action((source, destination) => {
      const log = new Logger('BackbeatSetup').newRequestLogger();
      const s = new _SetupReplication(source, destination, config);
      s.run(err => {
          if (err) {
              return log.error('error setting up replication', {
                  method: 'commander.action',
                  error: err.message,
                  errStack: err.stack,
              });
          }
          return log.info('replication setup successfully');
      });
  });

commander.parse(process.argv);
if (!commander.args.length) {
    commander.help();
    process.exit(1);
}
