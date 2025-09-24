#!/bin/bash

HADOOP_AWS_VERSION=3.3.2
AWS_SDK_VERSION=1.12.262

# Run Spark with local jars
echo "Starting Spark application..."
time spark-submit --jars ./jar_files/hadoop-aws-${HADOOP_AWS_VERSION}.jar,./jar_files/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar ./main.py