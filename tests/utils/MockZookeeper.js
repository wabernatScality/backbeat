const http = require('http');
const EventEmitter = require('events').EventEmitter;


class MockZookeeper extends EventEmitter {
    constructor() {
        super();
        return this;
    }

    checkPartitionOwnership(id, groupId, topic, partition, cb) {
        setImmediate(cb);
    }

    deletePartitionOwnership(groupId, topic, partition, cb) {
        setImmediate(cb);
    }

    isConsumerRegistered(groupId, id, cb) {
        setImmediate(cb);
    }

    addPartitionOwnership(id, groupId, topic, partition, cb) {
        setImmediate(cb);
    }

    registerConsumer(groupId, id, payloads, cb) {
        setImmediate(cb);
    }

    unregisterConsumer(groupId, id, cb) {
        setImmediate(cb);
    }

    // eslint-disable-next-line no-unused-vars
    listPartitions(topic) {}

    // eslint-disable-next-line no-unused-vars
    listConsumers(groupId) {}

    getConsumersPerTopic(groupId, cb) {
        const consumerTopicMap = {};
        consumerTopicMap[groupId] = ['fake-topic'];
        const topicConsumerMap = { 'fake-topic': [groupId] };
        const topicPartitionMap = { 'fake-topic': ['0', '1', '2'] };
        const map = {
            consumerTopicMap,
            topicConsumerMap,
            topicPartitionMap,
        };

        setImmediate(cb, null, map);
    }
}

class MockZKClient extends EventEmitter {
    constructor() {
        super();
        this.zk = new MockZookeeper();
        return this;
    }

    topicExists(topics, cb) {
        setImmediate(cb);
    }

    refreshMetadata(topicNames, cb) {
        setImmediate(cb);
    }

    sendOffsetCommitRequest(groupId, commits, cb) {
        setImmediate(cb);
    }

    /* eslint-disable no-unused-vars */
    sendFetchRequest(consumer, payloads, fetchMaxWaitMs, fetchMinBytes,
        maxTickMessages) {}
    /* eslint-enable */

    sendOffsetFetchRequest(groupId, payloads, cb) {
        setImmediate(cb);
    }

    sendOffsetRequest(payloads, cb) {
        setImmediate(cb);
    }

    addTopics(topics, cb) {
        setImmediate(cb);
    }

    removeTopicMetadata(topics, cb) {
        setImmediate(cb);
    }

    close(cb) {
        setImmediate(cb);
    }
}

class MyMocker {
    constructor() {
        this.zkClient = new MockZKClient();
    }

    onRequest(req, res) {

        console.log('received a request')
    }
}

const mock = new MyMocker();

const httpServer = http.createServer(
    (req, res) => mock.onRequest(req, res));
httpServer.listen(2181);
