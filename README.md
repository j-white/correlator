# Correlator

## About

Prototype used to perform correlation on time series stored in [Newts](http://newts.io) using [Spark](https://spark.apache.org).

The goal is to be able to perform sample selection and aggregation using the Cassandra connector and Spark's RDDs.

## Cassandra Setup

Using Cassandra 2.1.7

## Setting up Spark

Download and build:

    wget http://download.nextag.com/apache/spark/spark-1.4.0/spark-1.4.0.tgz
    tar zxvf spark-1.4.0.tgz
    cd spark-1.4.0/
    mvn -Pyarn -Phadoop-2.2 -Dhadoop.version=2.2.0 -DskipTests clean package

Start the master and a slave:

    ./sbin/start-master.sh
    ./sbin/start-slave.sh spark://$HOSTNAME:7077

You should now be able to access http://127.0.0.1:8080/. Make sure you have a worker registered.

## OSGi Stuff

Setting up your container:

    config:edit org.opennms.newts.persistence.cassandra
    config:property-set cassandra.keyspace newts
    config:property-set cassandra.hostname 45.55.88.223
    config:property-set cassandra.port 9042
    config:update

    feature:repo-add mvn:org.opennms.newts/newts-karaf/1.2.1-SNAPSHOT/xml/features
    feature:install newts-cassandra
    feature:install newts-cassandra-search

    feature:repo-add cxf
    feature:install cxf-commands

    feature:repo-add mvn:org.opennms.correlator/correlator-karaf/1.0.0-SNAPSHOT/xml/features
    feature:install correlator-newts
    feature:install correlator-rest

    feature:install war
    bundle:install -s "webbundle:mvn:org.opennms.correlator/correlator-ui/1.0.0-SNAPSHOT/war?Bundle-SymbolicName=correlator-ui&Web-ContextPath=/correlator"

You can now access the UI via:

    http://localhost:8181/correlator/app/index.html

Or access the REST service via:

    http://localhost:8181/cxf/karafsimple/correlator/correlate?resource=snmp%3Afs%3ANODES%3Any-cassandra-3%3Amib2-tcp&metric=tcpCurrEstab&resolution=36000000


## Spark Job Server

Setup sbt (the RPMs contain an older version):

    wget https://dl.bintray.com/sbt/native-packages/sbt/0.13.8/sbt-0.13.8.tgz
    tar zxvf sbt-0.13.8.tgz
    export PATH=$PATH:`pwd`/sbt/bin

Grab a copy of the Spark Job Server:

    git clone https://github.com/spark-jobserver/spark-jobserver.git
    cd spark-jobserver

Compile it using Scala 2.11:

    echo 'scalaVersion := "2.11.6"' > build.sbt
    echo 'scalaVersion in ThisBuild := "2.11.6"' >> build.sbt
    echo 'ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }' >> build.sbt
    sbt job-server/compile

Run it:

    sbt
    > reStart

Upload a job with:

    cd ~/git/correlator/spark/app
    ./run-job.sh
