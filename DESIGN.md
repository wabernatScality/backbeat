## FEATURE GOALS

* REP.1: (Active/Passive) Async Replication of objects at a different site/region/tenant
Typical workflow: Objects’ metadata are fetched from MD log and queued in Backbeat. Backbeat processes this queue and manages the transmission of objects to remote sites processing the records in FIFO.

* REP.2: (Active/Active) Async two-way Replication of objects on multiple sites

* ILM.1: Object Lifecycle Management (tiering/expiring)
Configuration to be applied on new objects but also possibly on existing objects in case of policy change. Typical workflow - From time to time, we examine bucket metadata to see if there is a lifecycle policy and all objects’ metadata to see if there is an applicable tag. If the policy (and tag) is applicable to an object, we pull the entries regarding the object from the log and the appropriate task is queued in Backbeat. Backbeat then processes this queue to eventually store the objects in a secondary tier.

* UTAPI.3: Store metrics (Utapi publishes to Backbeat)
S3 uses Utapi to publish metrics, which will queue the metric action to Backbeat and backbeat is responsible for storing this metric agnostic to Utapi/S3. Backbeat will batch process these events and sync to Redis to persist in regular intervals (every 5 minutes). This makes sure we never lose events.

* GC:  Perform some sort of GC for S3 to take care of orphans in data (and perhaps on cloud backends such as initiate MPU orphans on AWS S3)

## DESIGN CONSIDERATIONS
- Containerized - It lives in its own container
- Distributed - There will be 3-5 instances per site for HA. If one of the instances dies, the queue replica from another instance can be used to process the records.
- Extensions - To achieve features like replication, lifecycle etc. extensions can be written for the core engine
- Backend agnostic - All interactions will go through S3. This way we are not restricted to one backend or we don’t have to cook solutions for different backends. S3 would expose separate “fastpath” routes on a different port controlled by an accessKey/secretKey, these routes would only be used by Backbeat.
- Background Jobs - Backbeat will include a background process which runs on a crontab schedule. For example, the process “wakes up” at a set time and gets a list of buckets, gets attributes for a bucket, checks lifecycle policy, checks object metadata for tags if tags are used in a policy and then applies the lifecycle action to the matched objects.
- Kafka - Uses Kafka as the queue - pub/sub mechanism since it seems to match most of our requirements.
Will be written using node.js

## DESIGN OVERVIEW
![design](/res/design-overview.png)

### DEPLOYMENT (Mono/Multi-site)
The nodes of a Backbeat cluster must live within one given datacenter. Ideally there will be one standalone cluster per site.

Backbeat will be the backbone of features like active/active geo which rely on such local services to perform remote actions. The basis of active/active geo relies on versioning (S3-VER.1), Metadata and Data propagation from site to site, which does not need the message queue to be shared between sites (and we don’t want that either because the internals of Backbeat are not necessarily geo-aware: e.g. Zookeeper for Kafka).

### KAFKA PROS/CONS
Distributed, Pub/Sub, Simple to use, durable replicated log
Fast reads and writes using sequential reads/writes to disk
Topics contain partitions which are strictly ordered.
Replication is done on partition level and can be controlled per partition
More than one consumer can process a partition achieving parallel processing.
Can have one consumer processing real time and other consumer processing in batch
Easy to add a large number of consumers without affecting performance
We can leverage stream processing for encryption, compression etc.
Uses Zookeeper but it’s not in the critical path
Needs optimization for fault tolerance
Not designed for high latency environments (we can restrict the cluster to majority site for multisite deployments)


## APPLYING BACKBEAT
### USING BACKBEAT FOR REP.1 (Active/Passive replication)
![crr](res/backbeat-crr.png)

For Cross Region Replication (CRR), we can use Backbeat to replicate the objects asynchronously from a bucket of one region to the other.
CRR can be achieved by using Backbeat as follows
All replication actions go through S3. S3 exposes hot path routes for MetaData and Data backends to Backbeat.
MetaData log is used as the source of truth.
When a CRR configuration is enabled on a bucket, the bucket name is stored in a transient bucket (crr<splitter>buckets) in MetaData.
A producer (background job) wakes up, let’s say every 30 min, requests from S3 the list of buckets which have CRR enabled. Then the producer retrieves the MetaData log and filters the entries for CRR buckets. These entries are then populated as records in a Kafka topic ordered by their sequence number.
The entries contain the sequence number as key and a stringified json with fields like action (PUT), object key, object metadata.
Each CRR bucket gets its own partition in the Kafka topic (topic: crr). This ensures ordering of records within a partition.
A consumer is assigned to each partition, checking the partition regularly (lets say every 1 minute) processing the records in a batch. Parallelism is achieved by employing multiple consumers, each being responsible for their own bucket. The consumer sends the PUT object metadata request directly to the S3 on the replica site.
Reference: AWS rules of what is and what is not replicated http://docs.aws.amazon.com/AmazonS3/latest/dev/crr-what-is-isnot-replicated.html

### USING BACKBEAT FOR ILM.1 (Object Lifecycle Management)
![crr](res/backbeat-lifecycle.png)

When a lifecycle configuration is enabled on a bucket, objects that match the configuration are tiered/expired per life cycle rules.

Object Lifecycle Management can be achieved through Backbeat as follows
All lifecycle actions go through S3. S3 exposes hot path routes for MetaData and Data backends to Backbeat.
A lifecycle producer wakes up, let’s say every 24 hours and gets a list of buckets from S3.
The producer then walks through each bucket, if the bucket has life cycle configuration, it gets object metadata matching the config and makes an entry in the lifecycle Kafka topic (topic:lifecycle).
The entry would contain fields like nature of operation (PUT/DEL), object metadata.
All entries go into a topic with keys being partitioned automatically by Kafka.
Multiple consumers would then be started processing the records in the topic.
The consumers are responsible for tiering/deleting the data and updating/deleting object metadata according to the action.
High throughput is achieved by using multiple consumers running in parallel working on the lifecycle topic.

### USING BACKBEAT FOR UTAPI.3
![crr](res/backbeat-utapi.png)

Backbeat can be used to avoid loss of metrics in Utapi due to Redis downtime or other issues.

The push metric process of Utapi can be re-designed as follows to avoid loss of metrics.
S3 successfully processes an api request and publishes to the push metric channel (topic:utapi) with the metrics details.
A consumer wakes up every few minutes, let’s say every 5 minutes, processes records in the topic and publishes the metrics for persistent storage in Redis.
If Redis is down, the metrics are retried at a later time.
Ordering of records doesn’t matter as it evens out once all the records are applied. So multiple consumers can be used to process the topic records in parallel.




## SECURITY
- AWS uses IAM roles with IAM policies to gain access to buckets for CRR, Lifecycle actions and follows a similar path of how they provide third parties access to AWS resources. This setup can be translated to our use case as follows.
- Use PKI certificates with TLS for trusted communication between the services Backbeat and Vault.
On all the sites(SF, NY, Paris for example), communication to Vault occurs through a private route by establishing a TCP connection using the PKI certificates.
- Vault will be pre-configured with a user called scality-s3. This user is will have no credentials (access key, secret key pair) assigned by default and will be used by Backbeat.
- A trust policy (IAM actions) will be created allowing the user to assume the “role” gain temporary credentials to the source/destination accounts’ resources.
- Cross account permissions can be managed using Bucket ACLs.
Access policies (S3 actions) will be created allowing the “role” to perform specific actions to achieve CRR or Lifecycle actions.
Backbeat first requests temporary credentials from Vault through the private route intended for Backbeat after establishing a trusted communication connection using the certificates. Vault creates temporary credentials for the user scality-s3 assuming the “role” which can be stored by Vault in Redis. These temporary credentials expire every few hours(or some set time) in Redis emulating STS.
These credentials will be used to communicate with S3 for performing life cycle, replication actions.
- Backbeat would renew credentials from Vault when they expire.
Temporary credentials don’t span across the sites, so Backbeat has to renew credentials on all the sites individually.
- The above setup is inline with AWS’ behavior, security practices and it would not require us to re-invent the wheel. Benefits are that we don’t hardcode any credentials in Backbeat and roles, policies are transparent to the customer who can see what actions we are using for replication or lifecycle. Customers are not expected to maintain or alter these policies, they will be managed by Scality at deploy time.

## OSS LICENSES
Kafka and Zookeeper are the only dependencies of Backbeat which are licensed under Apache License 2.0.
Node-kafka npm module which will be used for the producer/consumer actions is licensed under MIT license.

## STATISTICS FOR SLA, METRICS etc.
There are two ways we can approach this.
Pub/Sub events can be used in addition to the MetaData log in a separate topic (let’s call it statistics). The records in this topic can be leveraged by comparing to the active queue to generate statistics like
RPO (Recovery Point Objective)
RTO(Recovery Time Objective)
Storage backlog in bytes
Storage replicated so far in bytes
Number of objects in backlog
Number of objects replicated so far etc.
Use a decoupled topic in addition to the queue topic. This will be managed by the producers/consumers adding records for non-replicated and replicated entries. Since each entry would have a sequence number, calculating the difference between the sequence numbers of the latest non-replicated and replicated records would give us the required statistics.




## VAULT ASSUMPTIONS
Backbeat design works under the assumption that there is one Vault for multiple sites located in different Geological regions (Need to converge on implementation details as a separate project orthogonal to Backbeat)
IAM roles - to be implemented (plan is to implement a minimal core concept of roles to be used internally within Vault without implementing any APIs)
Policies for IAM - to be implemented (incremental work using our current policy engine)
