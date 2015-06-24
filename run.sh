#!/bin/sh
mvn -DskipTests=true clean package
java -jar assembly/target/correlator-assembly-1.0.0-SNAPSHOT.jar server correlator.yml
