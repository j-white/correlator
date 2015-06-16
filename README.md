# Correlator

## About

Prototype used to perform correlation on time series stored in [Newts](http://newts.io).

## Setup Spark

Download and build:
 
    wget http://download.nextag.com/apache/spark/spark-1.4.0/spark-1.4.0.tgz
    tar zxvf spark-1.4.0.tgz
    cd spark-1.4.0/
    mvn -Pyarn -Phadoop-2.2 -Dhadoop.version=2.2.0 -DskipTests clean package

Start the master and a slave:

    ./sbin/start-master.sh
    ./sbin/start-slave.sh spark://$HOSTNAME:7077

You should now be able to access http://127.0.0.1:8080/, make sure you have a worker registered.

