#!/bin/bash
set -euo pipefail

HADOOP_AWS_VERSION=3.3.2
AWS_SDK_VERSION=1.12.262

HADOOP_AWS_URL="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
AWS_SDK_URL="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

mkdir -p ./jar_files
cd ./jar_files

# Download jars if they don't exist
if [ ! -f hadoop-aws-${HADOOP_AWS_VERSION}.jar ]; then
    echo "Downloading hadoop-aws..."
    if ! curl -fSL -o hadoop-aws-${HADOOP_AWS_VERSION}.jar "$HADOOP_AWS_URL"; then
        echo "ERROR: Failed to download hadoop-aws"
        exit 1
    fi
else
    echo "hadoop-aws .jar file arleady downloaded..."
fi

if [ ! -f aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar ]; then
    echo "Downloading aws-java-sdk-bundle..."
    if ! curl -fSL -o aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar "$AWS_SDK_URL"; then
        echo "ERROR: Failed to download aws-java-sdk-bundle"
        exit 1
    fi
else
    echo "aws-java-sdk-bundle .jar file arleady downloaded..."
fi

echo "All jars downloaded successfully."
