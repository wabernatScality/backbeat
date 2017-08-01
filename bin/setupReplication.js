const async = require('async');
const commander = require('commander');
const { S3, IAM, SharedIniFileCredentials } = require('aws-sdk');

const { Logger } = require('werelogs');

const config = require('../conf/Config');

const trustPolicy = {
    Version: '2012-10-17',
    Statement: [
        {
            Effect: 'Allow',
            Principal: {
                Service: 'backbeat',
            },
            Action: 'sts:AssumeRole',
        },
    ],
};

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
        region: 'file',
    });
}


function _setupIAMClient(host, port, profile) {
    const credentials = new SharedIniFileCredentials({ profile });
    return new IAM({
        endpoint: `http://${host}:${port}`,
        sslEnabled: false,
        credentials,
        maxRetries: 0,
        region: 'file',
        signatureCache: false,
        s3ForcePathStyle: true,
        httpOptions: {
            timeout: 1000,
        },
    });
}

class _SetupReplication {
    /**
     * This class sets up two buckets for replication.
     * @constructor
     * @param {String} sourceBucket - Source Bucket Name
     * @param {String} targetBucket - Destination Bucket Name
     * @param {Object} log - Werelogs Request Logger object
     * @param {Object} config - bucket configurations
     */
    constructor(sourceBucket, targetBucket, log, config) {
        const { source, destination } = config.extensions.replication;
        this._log = log;
        this._sourceBucket = sourceBucket;
        this._targetBucket = targetBucket;
        this._s3 = {
            source: _setupS3Client(source.s3.host, source.s3.port,
                'backbeatsource'),
            target: _setupS3Client(destination.s3.host, destination.s3.port,
                'backbeattarget'),
        };
        this._iam = {
            source: _setupIAMClient(source.auth.vault.host,
                 source.auth.vault.iamPort,
                'backbeatsource'),
            target: _setupIAMClient(destination.auth.vault.host,
                 destination.auth.vault.iamPort,
                'backbeattarget'),
        };
    }

    _checkSanity(cb) {
        async.waterfall({
            first: next => this._isValidBucket('source', next),
            second: next => this._isValidBucket('target', next),
            third: next => this._isVersioningEnabled('source', next),
            fourth: next => this._isVersioningEnabled('target', next),
            fifth: next => this._isReplicationEnabled('source', next),
            sixth: (next, srcArn, tgtArn) => {
                this._isValidRole('source', srcArn, err => {
                    if (err) next(err);
                });
                this._isValidRole('target', tgtArn, next);
            },
        }, cb);
    }

    _isValidBucket(where, cb) {
        // Does the bucket exist and is it reachable?
        const bucket = where === 'source' ? this._sourceBucket :
            this._targetBucket;
        this._s3[where].headBucket({ Bucket: bucket }, err => {
            if (err) {
                return cb(err);
            }
            return cb(null);
        });
    }

    _isVersioningEnabled(where, cb) {
        // Does the bucket have versioning enabled?
        const bucket = where === 'source' ? this._sourceBucket :
            this._targetBucket;
        this._s3[where].getBucketVersioning({ Bucket: bucket }, (err, res) => {
            if (err || res.Status === 'Disabled') {
                return cb(err);
            }
            return cb(null);
        });
    }

    _isValidRole(where, arn, cb) {
        // Is the role mentioned in the replication config available in IAM

        // Goal is to get Role given known ARN.
        // If err, there is no matching role
        const roleName = arn.split('/').pop();
        this._iam[where].getRole({ RoleName: roleName }, (err, res) => {
            if (err || arn !== res.Role.Arn) {
                return cb(err);
            }
            return cb(null);
        });
    }

    _isReplicationEnabled(src, cb) {
        // Is the Replication config enabled?
        this._s3[src].getBucketReplication({ Bucket: src }, (err, res) => {
            const r = res.ReplicationConfiguration;
            if (err || r.Rules[0].Status === 'Disabled') {
                return cb(err);
            }
            return cb(null, r.Role, r.Rules[0].Destination.Bucket);
        });
    }

    _createBucket(where, cb) {
        const bucket = where === 'source' ? this._sourceBucket :
            this._targetBucket;
        this._s3[where].createBucket({ Bucket: bucket }, (err, res) => {
            if (err) {
                this._log.error('error creating a bucket', {
                    method: '_SetupReplication._createBucket',
                    error: err.message,
                    errStack: err.stack,
                });
                return cb(err);
            }
            this._log.debug('Created bucket', {
                bucket: where,
                response: res,
                method: '_createBucket',
            });
            return cb(null, err);
        });
    }

    _createRole(where, cb) {
        const params = {
            AssumeRolePolicyDocument: JSON.stringify(trustPolicy),
            RoleName: `bb-replication-${Date.now()}`,
            Path: '/',
        };

        this._iam[where].createRole(params, (err, res) => {
            if (err) {
                this._log.error('error creating a role', {
                    method: '_SetupReplication._createRole',
                    error: err.message,
                    errStack: err.stack,
                });
                return cb(err);
            }
            this._log.debug('Created role', {
                bucket: where,
                response: res,
                method: '_createRole',
            });
            return cb(null, res);
        });
    }

    _createPolicy(where, cb) {
        const params = {
            PolicyDocument: JSON.stringify(
                _buildResourcePolicy(this._sourceBucket, this._targetBucket)),
            PolicyName: `bb-replication-${Date.now()}`,
        };
        this._iam[where].createPolicy(params, (err, res) => {
            if (err) {
                this._log.error('error creating policy', {
                    method: '_SetupReplication._createPolicy',
                    error: err.message,
                    errStack: err.stack,
                });
                return cb(err);
            }
            this._log.debug('Created policy', {
                bucket: where,
                response: res,
                method: '_createPolicy',
            });
            return cb(null, res);
        });
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
        this._s3[where].putBucketVersioning(params, (err, res) => {
            if (err) {
                this._log.error('error enabling versioning', {
                    method: '_SetupReplication._enableVersioning',
                    error: err.message,
                    errStack: err.stack,
                });
                return cb(err);
            }
            this._log.debug('Versioning enabled', {
                bucket: where,
                response: res,
                method: '_enableVersioning',
            });
            return cb(null, res);
        });
    }

    _attachResourcePolicy(policyArn, roleName, where, cb) {
        const params = {
            PolicyArn: policyArn,
            RoleName: roleName,
        };
        this._iam[where].attachRolePolicy(params, (err, res) => {
            if (err) {
                this._log.error('error attaching resource policy', {
                    method: '_SetupReplication._attachResourcePolicy',
                    error: err.message,
                    errStack: err.stack,
                });
                return cb(err);
            }
            this._log.debug('Attached resource policy', {
                bucket: where,
                response: res,
                method: '_attachResourcePolicy',
            });
            return cb(null, res);
        });
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
        this.s3.source.putBucketReplication(params, (err, res) => {
            if (err) {
                this._log.error('error enabling replication', {
                    method: '_SetupReplication._enableReplication',
                    error: err.message,
                    errStack: err.stack,
                });
                return cb(err);
            }
            this._log.debug('Bucket replication enabled', {
                response: res,
                method: '_enableReplication',
            });
            return cb(null, res);
        });
    }

    _parallelTasks(cb) {
        async.parallel({
            sourceBucket: next => this._createBucket('source', next),
            targetBucket: next => this._createBucket('target', next),
            sourceRole: next => this._createRole('source', next),
            targetRole: next => this._createRole('target', next),
            sourcePolicy: next => this._createPolicy('source', next),
            targetPolicy: next => this._createPolicy('target', next),
        }, (err, res) => {
            cb(err, res);
        });
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
            next => this._parallelTasks((err, setupInfo) => {
                next(err, setupInfo);
            }),
            (setupInfo, next) => this._seriesTasks(setupInfo, next),
            next => this._checkSanity(next),
        ], cb);
    }
}


function _crBucket(s3Instance, bucket, cb) {
    s3Instance.createBucket({ Bucket: bucket }, (err, data) => {
        if (err) {
            cb(err);
        } else {
            cb(null);
        }
    });
}

function _crRole(iamInstance, cb) {
    const params = {
        AssumeRolePolicyDocument: JSON.stringify(trustPolicy),
        RoleName: `bb-replication-${Date.now()}`,
        Path: '/',
    };
    iamInstance.createRole(params, (err, res) => {
        if (err) {
            return cb(err);
        }
        return cb(null, res);
    });
}

commander
  .version('0.1.0')
  .arguments('<source> <destination>')
  .action((source, destination) => {
      const log = new Logger('BackbeatSetup').newRequestLogger();
      const s = new _SetupReplication(source, destination, log, config);
      s.run(err => {
          if (err) {
              return log.info('replication script failed');
          }
          return log.info('replication setup successful');
      });

    //   const src = config.extensions.replication.source;
    //   const dst = config.extensions.replication.destination;
    //   const s = _setupS3Client(src.s3.host, src.s3.port,
    //         'backbeatsource');
    //   const d = _setupS3Client(dst.s3.host, dst.s3.port,
    //         'backbeattarget');
    //   const sc = _setupIAMClient(src.auth.vault.host,
    //        src.auth.vault.iamPort,
    //       'backbeatsource');
    //   const dc = _setupIAMClient(dst.auth.vault.host,
    //        dst.auth.vault.iamPort,
    //       'backbeattarget');
      //
    //   async.series({
    //       one: next => _crBucket(s, source, (err, res) => {
    //           if (err) {
    //               return next(err);
    //           }
    //           console.log('success in one');
    //           return next(null, res);
    //       }),
    //       two: next => _crBucket(d, destination, (err, res) => {
    //           if (err) {
    //               return next(err);
    //           }
    //           console.log('success in two');
    //           return next(null, res);
    //       }),
    //       three: next => _crRole(dc, (err, res) => {
    //           if (err) {
    //               return next(err);
    //           }
    //           console.log('success in three');
    //           return next(null, res);
    //       }),
    //       four: next => _crRole(sc, (err, res) => {
    //           if (err) {
    //               return next(err);
    //           }
    //           console.log('success in four');
    //           return next(null, res);
    //       }),
    //   }, (err, suc) => {
    //       if (err) {
    //           console.log(err);
    //       } else {
    //           console.log(suc);
    //       }
    //   })
  });

commander.parse(process.argv);
if (!commander.args.length) {
    commander.help();
    process.exit(1);
}
