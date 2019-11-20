#! /bin/sh
cd ~/ROI
export PYSPARK_SUBMIT_ARGS="--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=192.168.0.123,192.168.0.124 pyspark-shell"
jupyter notebook --allow-root

