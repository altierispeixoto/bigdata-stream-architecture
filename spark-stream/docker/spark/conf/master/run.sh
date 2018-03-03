#!/usr/bin/env bash

rm -rf /tmp/data/output/checkpoint/*

export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=4000

#spark-submit --name JavaStructuredSessionization --master spark://master:7077 --class JavaStructuredSessionization /tmp/java/spark-stream-1.0-SNAPSHOT.jar

spark-submit --name JavaStatefulNetworkWordCount --master spark://master:7077 --class JavaStatefulNetworkWordCount /tmp/java/spark-stream-1.0-SNAPSHOT.jar