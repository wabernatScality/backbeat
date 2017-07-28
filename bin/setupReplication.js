const async = require('async');
const commander = require('commander');
const { S3, IAM, SharedIniFileCredentials } = require('aws-sdk');

const config = require('../conf/Config');

const { source, destination } = config.extensions.replication;
const sourceCredentials = new SharedIniFileCredentials({profile: 'backbeatsource'});
const destinationCredentials = new SharedIniFileCredentials({profile: 'backbeatdestination'});


//////////////////////
// Commander Stuffs //
//////////////////////
// NOTE: Use commander to initiate action of the script
// Wrap the async and setup calls into a function

let sourceName;
let destinationName;

commander
  .version('0.1.0')
  // .usage('<source> <destination>')
  .arguments('<source> <destination>')
  .action(function (source, destination) {
     sourceName = source;
     destinationName = destination;
 });

commander.parse(process.argv);
if (!commander.args.length) {
    commander.help()
    process.exit(1);
}


//////////////////////////////
// Instantiate S3 Instances //
//////////////////////////////
const sourceOptions = {
    endpoint: `http://${source.s3.host}:${source.s3.port}`,
    sslEnabled: false,
    credentials:  sourceCredentials,
    s3ForcePathStyle: true,
}

const destinationOptions = {
    endpoint: `http://${destination.s3.host}:${destination.s3.port}`,
    sslEnabled: false,
    credentials: destinationCredentials,
    s3ForcePathStyle: true,
    // region: 'us-west-1'
}

const sourceS3 = new S3(sourceOptions);
const destinationS3 = new S3(destinationOptions);

const iamSource = new IAM(sourceOptions);
const iamDestination = new IAM(destinationOptions);


/////////////////////////
// Hard-Coded Policies //
/////////////////////////
// TRUST POLICY
const trustPolicy = {
   "Version":"2012-10-17",
   "Statement":[
      {
         "Effect":"Allow",
         "Principal":{
            "Service":"backbeat"  /* "s3.amazonaws.com" */
         },
         "Action":"sts:AssumeRole"
      }
   ]
}

// RESOURCE POLICY
const resourcePolicy = {
    "Version":"2012-10-17",
    "Statement":[
        {
            "Effect":"Allow",
            "Action":[
                "s3:GetObjectVersion",
                "s3:GetObjectVersionAcl"
            ],
            "Resource":[
                `arn:aws:s3:::${source}/*`
            ]
        },
        {
            "Effect":"Allow",
            "Action":[
                "s3:ListBucket",
                "s3:GetReplicationConfiguration"
            ],
            "Resource":[
                `arn:aws:s3:::${source}`
            ]
        },
        {
            "Effect":"Allow",
            "Action":[
                "s3:ReplicateObject",
                "s3:ReplicateDelete"
            ],
            "Resource":`arn:aws:s3:::${destination}/*`
        }
    ]
}


//////////////////////
// Helper Functions //
//////////////////////

// CREATE Functions
function _createBucket(s3Instance, bucket) {
    s3Instance.createBucket({ Bucket: bucket }, (err, data) => {
        if (err) {
            throw err;
        } else {
            console.log(data);
        }
    });
}

function _createRole(iamInstance, name, source, destination, next) {
    const validPolicy = encodeURIComponent(JSON.stringify(resourcePolicy));

    let params = {
        AssumeRolePolicyDocument: validPolicy,
        RoleName: name,  // NOTE: Refactor out 'name' param
        Path: '/'
    };

    iamInstance.createRole(params, (err, data) => {
        if (err) {
            next(err);
        } else {
            next(null, data);
        }
    });
}

function _createPolicy(iamInstance, policyName, next) {
    const params = {
        PolicyDocument: JSON.stringify(resourcePolicy),
        PolicyName: policyName
    }

    iamInstance.createPolicy(params, (err, data) => {
        if (err) {
            next(err);
        } else {
            next(null, data);
        }
    })
}

// EDIT/PUT Functions
function _addVersioning(s3Instance, bucket, next) {
    let params = {
        Bucket: bucket,
        VersioningConfiguration: {
            Status: 'Enabled'
        }
    }

    s3Instance.putBucketVersioning(params, (err, data) => {
        if (err) {
            next(err);
        } else {
            next(null, data);
        }
    })
}

function _attachRolePolicy(iamInstance, arn, name) {
    const params = {
        PolicyArn: arn,
        RoleName: name
    }

    iamInstance.attachRolePolicy(params, (err, data) => {
        // NOTE: Throwing here because outside of async.parallel
        if (err) throw(err);

        console.log(data);
    })
}

function _addReplication(s3Instance, arn, source, destination) {
    let params = {
        Bucket: source,
        ReplicationConfiguration: {
            Role: arn,
            Rules: [{
                Destination: {
                    Bucket: `arn:aws:s3:::${destination}`
                },
                Prefix: '',
                Status: Enabled
            }]
        }
    }

    s3Instance.putBucketReplication(params, (err, data) => {
        // NOTE: Throwing here because outside of async.parallel
        if (err) throw (err);

        console.log(data);
    })
}


///////////////
// Do stuffs //
///////////////

// NOTE: Don't think I need to do series for most operations here.
// The return data is not used in many cases except for roles.

// create source bucket, target bucket (done)
// enable versioning for source and target bucket (done)
// create roles on source and target with assume role trust policy (done)

// create resource policies allowing replication actions for BB on source and target
// attach these policies to the roles created in the prior step
// enable replication configuration on the source bucket with the config including the role details and target bucket name.

// async.parallel
async.series({
    bucket1: (next) => {
        _createBucket(sourceS3, sourceName);
        _addVersioning(sourceS3, sourceName, next);
    },
    bucket2: (next) => {
        _createBucket(destinationS3, destinationName);
        _addVersioning(destinationS3, destinationName, next);
    },
    // sourceRole: (next) => {
    //     _createRole(iamSource, 'Test-Source', sourceName, destinationName, next);
    //     /*
    //     data = {
    //      Role: {
    //       Arn: "arn:aws:iam::123456789012:role/Test-Role",
    //       AssumeRolePolicyDocument: "<URL-encoded-JSON>",
    //       CreateDate: <Date Representation>,
    //       Path: "/",
    //       RoleId: "AKIAIOSFODNN7EXAMPLE",
    //       RoleName: "Test-Role"
    //      }
    //     }
    //     */
    // },
    // destinationRole: (next) => {
    //     _createRole(iamDestination, 'Test-Destination', sourceName, destinationName, next);
    // },
    // sourcePolicy: (next) => {
    //     _createPolicy(iamSource, 'sourcePolicy', next);
    // },
    // destinationPolicy: (next) => {
    //     _createPolicy(iamDestination, 'destinationPolicy', next);
    // }
}, (err, result) => {
    if (err) throw (err);

    console.log("ending;")
    console.log(result);

    // const sourceArn = result.sourcePolicy.Policy.Arn;
    // const destinationArn = result.destinationPolicy.Policy.Arn;
    // const sourceRoleName = result.sourceRole.Role.RoleName;
    // const destinationRoleName = result.destinationRole.Role.RoleName;
    //
    // // Attach policies to roles
    // //_attachRolePolicy(iamInstance, arn, name, next) {
    // _attachRolePolicy(iamSource, sourceArn, sourceRoleName);
    // _attachRolePolicy(iamDestination, destinationArn, destinationRoleName);
    //
    // // Enable replication
    // addReplication(sourceS3, sourceArn, sourceName, destinationName);
})





//
// "source": {
//     "s3": {
//         "host": "127.0.0.1",
//         "port": 8000,
//         "transport": "https"
//     },
//     "auth": {
//         "type": "account",
//         "account": "bart",
//         "vault": {
//             "host": "127.0.0.1",
//             "port": 8500
//         }
//     },
//     "logSource": "dmd",
//     "bucketd": {
//         "host": "127.0.0.1",
//         "port": 9000,
//         "raftSession": 1
//     },
//     "dmd": {
//         "host": "127.0.0.1",
//         "port": 9990
//     }
// },
// "destination": {
//     "s3": {
//         "host": "127.0.0.2",
//         "port": 9000,
//         "transport": "https"
//     },
//     "auth": {
//         "type": "account",
//         "account": "lisa",
//         "vault": {
//             "host": "127.0.0.2",
//             "port": 9500
//         }
//     },
//     "certFilePaths": {
//         "key": "ssl/key.pem",
//         "cert": "ssl/cert.crt",
//         "ca": "ssl/ca.crt"
//     }
// },
// "topic": "backbeat-replication",
// "queuePopulator": {
//     "cronRule": "*/5 * * * * *",
//     "batchMaxRead": 10000,
//     "zookeeperPath": "/replication-populator"
// },
// "queueProcessor": {
//     "groupId": "backbeat-replication-group"
// }
//
