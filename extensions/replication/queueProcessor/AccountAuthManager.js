const assert = require('assert');
const AWS = require('aws-sdk');

const errors = require('arsenal').errors;
const ARN = require('arsenal').models.ARN;

const authdata = require('../../../conf/authdata.json');

class AccountAuthManager {
    constructor(authConfig, log) {
        assert.strictEqual(authConfig.type, 'account');

        this._log = log;
        const accountInfo = authdata.accounts.find(
            account => account.name === authConfig.account);
        if (accountInfo === undefined) {
            throw Error(`No such account registered: ${authConfig.account}`);
        }
        if (accountInfo.arn === undefined) {
            throw Error(`Configured account ${authConfig.account} has no ` +
                        '"arn" property defined');
        }
        if (accountInfo.canonicalID === undefined) {
            throw Error(`Configured account ${authConfig.account} has no ` +
                        '"canonicalID" property defined');
        }
        if (accountInfo.displayName === undefined) {
            throw Error(`Configured account ${authConfig.account} has no ` +
                        '"displayName" property defined');
        }
        const accountArn = ARN.createFromString(accountInfo.arn);
        if (accountArn.error !== undefined) {
            throw Error(`Configured account ${authConfig.account} has a ` +
                        `bad ARN "${accountInfo.arn}": ${accountArn.error}`);
        }
        if (!accountArn.isIAMAccount()) {
            throw Error(`Configured account ${authConfig.account} ARN is ` +
                        'not an account ARN');
        }
        this._accountArn = accountArn;
        this._canonicalID = accountInfo.canonicalID;
        this._displayName = accountInfo.displayName;
        this._credentials = new AWS.Credentials(accountInfo.keys.access,
                                                accountInfo.keys.secret);
    }

    getCredentials() {
        return this._credentials;
    }

    lookupAccountAttributes(accountId, cb) {
        const localAccountId = this._accountArn.getAccountId();
        if (localAccountId !== accountId) {
            this._log.error('Target account for replication must match ' +
                            'configured destination account ARN',
                { targetAccountId: accountId,
                    localAccountId });
            return process.nextTick(() => cb(errors.AccountNotFound));
        }
        // return local account's attributes
        return process.nextTick(
            () => cb(null, { canonicalID: this._canonicalID,
                displayName: this._displayName }));
    }
}

module.exports = AccountAuthManager;
