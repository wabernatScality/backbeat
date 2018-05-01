const async = require('async');
const program = require('commander');
const { SharedIniFileCredentials } = require('aws-sdk');

const werelogs = require('werelogs');
const Logger = werelogs.Logger;
const { RoundRobin } = require('arsenal').network;

const config = require('../conf/Config');
const SetupReplication =
          require('../extensions/replication/utils/SetupReplication');
const version = require('../package.json').version;

werelogs.configure({
    level: config.log.logLevel,
    dump: config.log.dumpLevel,
});


function _createSetupReplication(command, options, log) {
    const sourceBucket = options.sourceBucket;
    const targetBucket = options.targetBucket;
    const sourceProfile = options.sourceProfile;
    const targetProfile = options.targetProfile;
    const siteName = options.siteName;
    const targetIsExternal = siteName !== undefined;

    // Required options
    if (!sourceBucket || !targetBucket ||
        !sourceProfile || (!targetProfile && !targetIsExternal)) {
        program.commands.find(n => n._name === command).outputHelp();
        process.stdout.write('\n');
        process.exit(1);
    }

    const { source, destination } = config.extensions.replication;
    const sourceCredentials =
              new SharedIniFileCredentials({ profile: sourceProfile });
    const targetCredentials =
              new SharedIniFileCredentials({ profile: targetProfile });
    const destinationEndpoint =
        config.getBootstrapList()
        .find(dest => Array.isArray(dest.servers));
    return new SetupReplication({
        source: {
            bucket: sourceBucket,
            credentials: sourceCredentials,
            s3: source.s3,
            vault: source.auth.vault,
            transport: source.transport,
        },
        target: {
            bucket: targetBucket,
            credentials: targetCredentials,
            hosts: targetIsExternal ?
                undefined : new RoundRobin(destinationEndpoint.servers),
            transport: destination.transport,
            isExternal: targetIsExternal,
            siteName,
        },
        checkSanity: true,
        log,
    });
}

program.version(version);

[
    {
        name: 'setup',
        method: 'setupReplication',
        errorLog: 'replication setup failed',
        successLog: 'replication setup successful',
    },
    {
        name: 'validate',
        method: 'checkSanity',
        errorLog: 'replication validation check failed',
        successLog: 'replication is correctly setup',
    },
].forEach(cmd => {
    program
        .command(cmd.name)
        .option('--source-bucket <name>', '[required] source bucket name')
        .option('--source-profile <name>',
                '[required] source aws/credentials profile')
        .option('--target-bucket <name>', '[required] target bucket name')
        .option('--target-profile <name>', '[optional] target ' +
                'aws/credentials profile (optional if using external location)')
        .option('--site-name <name>', '[optional] the site name (required if ' +
                'using external location)')
        .action(options => {
            const log = new Logger('BackbeatSetup').newRequestLogger();
            const s = _createSetupReplication(cmd.name, options, log);
            s[cmd.method](err => {
                if (err) {
                    log.error(cmd.errorLog, {
                        errCode: err.code,
                        error: err.message,
                    });
                    process.exit(1);
                }
                log.info(cmd.successLog);
                process.exit();
            });
        });
});

program.command('multi-setup')
    .option('--source-bucket <name>', '[required] source bucket name')
    .option('--source-profile <name>',
            '[required] source aws/credentials profile')
    .option('--multi-sites <site1,site2,...>', '[required] comma separated ' +
            'list of sites"')
    .option('--target-profile <name>', '[optional] target ' +
            'aws/credentials profile (this is in addition to multi-sites)')
    .action(options => {
        const log = new Logger('BackbeatSetup').newRequestLogger();
        const sourceBucket = options.sourceBucket;
        const sourceProfile = options.sourceProfile;
        const targetProfile = options.targetProfile;
        const multiSites = options.multiSites;

        // Required options
        if (!sourceBucket || !sourceProfile || !multiSites) {
            program.commands.find(n => n._name === 'multi-setup').outputHelp();
            process.stdout.write('\n');
            process.exit(1);
        }

        const sites = options.multiSites.split(',');
        let toProcessSites = sites.map(site => (
            {
                sourceBucket,
                sourceProfile,
                targetBucket: sourceBucket,
                siteName: site,
            }
        ));

        if (targetProfile) {
            toProcessSites.push({
                sourceBucket,
                sourceProfile,
                targetBucket: sourceBucket,
                targetProfile,
            });
        }

        toProcessSites = toProcessSites.map(site =>
            _createSetupReplication('setup', site, log));

        async.each(toProcessSites, (endpoint, next) => {
            endpoint.setupReplication(err => {
                if (err) {
                    log.error('replication setup failed', {
                        errCode: err.code,
                        error: err.message,
                        targetSiteName: endpoint.getTargetSiteName(),
                        targetBucketName: endpoint.getTargetBucketName(),
                    });
                    process.exit(1);
                    return next(err);
                }
                log.info('setup completed for bucket: ' +
                    `${endpoint.getTargetBucketName}`);
                return next();
            });
        }, err => {
            if (err) {
                log.error('replication setup for multiple sites failed');
                process.exit(1);
            }
            log.info('replication setup successful for multiple sites');
            process.exit();
        });
    });

const validCommands = program.commands.map(n => n._name);

// Is the command given invalid or are there too few arguments passed
if (!validCommands.includes(process.argv[2])) {
    program.outputHelp();
    process.stdout.write('\n');
    process.exit(1);
} else {
    program.parse(process.argv);
}
