#!/bin/bash

cd output/target

unzip gearpump-2.11-0.8.1-RC3.zip

cd gearpump-2.11-0.8.1-RC3/bin

chmod +x services
chmod +x gear
chmod +x local

./services &
./local &

./gear app -jar ../../../../examples/target/2.11/cassandra-2.11-0.8.1-RC3-assembly.jar org.apache.gearpump.streaming.examples.cassandra.CassandraTransform
