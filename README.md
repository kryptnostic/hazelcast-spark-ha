# hazelcast-spark-ha
Enables use of Hazelcast for Spark High Availability

## Prerequisites

1. Spark 1.6.2: http://spark.apache.org/downloads.html
2. Hazelcast 3.7: https://hazelcast.org/download/

## Getting Started

1. Clone the repo and run `./gradlew build shadow`
2. Include this jar on the Spark Master classpath by modify `conf/spark-env.sh` to include the jar from step 1
```bash
export SPARK_CLASSPATH=/Users/mtamayo/repos/kryptnostic/hazelcast-spark-ha/build/libs/hazelcast-spark-ha-all.jar
```
3. Configure the following lines in `conf/spark-defaults.conf` with the appropriate values for your setup.
```properties
     spark.deploy.recoveryMode CUSTOM
     spark.deploy.recoveryMode.factory com.kryptnostic.sparks.HazelcastRecoveryModeFactory
     com.kryptnostic.hazelcast.nodes <host1:port1,host2:port2>
```
