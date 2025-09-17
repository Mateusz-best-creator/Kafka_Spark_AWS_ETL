#!/bin/bash

HADOOP_AWS_VERSION=3.3.2
AWS_SDK_VERSION=1.12.262

HADOOP_AWS_URL="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
AWS_SDK_URL="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# Download jars if they don't exist
if [ ! -f ./hadoop-aws-${HADOOP_AWS_VERSION}.jar ]; then
    echo "Downloading hadoop-aws..."
    curl -L -o hadoop-aws-${HADOOP_AWS_VERSION}.jar $HADOOP_AWS_URL
fi

if [ ! -f ./aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar ]; then
    echo "Downloading aws-java-sdk-bundle..."
    curl -L -o aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar $AWS_SDK_URL
fi

# Run Spark with local jars
echo "Starting Spark application..."
spark-submit --jars ./hadoop-aws-${HADOOP_AWS_VERSION}.jar,./aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar main.py
