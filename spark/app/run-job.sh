#!/bin/sh
JAR="target/uber-correlator-spark-app-1.0.0-SNAPSHOT.jar"
URL="http://localhost:8090"
curl --data-binary @$JAR $URL/jars/correlator
curl -H "Content-Type: application/json" -X POST -d '{spark.context-settings: {spark.cores.max: 4, spark.cassandra.connection.host: "45.55.88.223", spark.cassandra.connection.port: 9042}, metric: {resource:"a.b", metric:"m"}, candidates:[{resource:"a.c", metric:"n"}], from: 1435268420973, to: 1435354820973, resolution: 10800000, topN: 10}' $URL'/jobs?appName=correlator&classPath=org.opennms.correlator.spark.Correlator&sync=true&timeout=300'
