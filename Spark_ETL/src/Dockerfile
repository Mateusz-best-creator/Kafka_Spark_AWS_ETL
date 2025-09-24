# Use Ubuntu as the base image for better compatibility
FROM ubuntu:22.04

# Define build arguments
ARG SPARK_VERSION=3.5.0
ARG HADOOP_VERSION=3.3.6
ARG SPARK_UID=185
ARG SPARK_GID=999

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop
ENV PATH="$SPARK_HOME/bin:$HADOOP_HOME/bin:$PATH"
ENV PYTHONUNBUFFERED=1

WORKDIR /opt

# Install system dependencies and Python 3
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    bash curl wget openjdk-11-jdk libstdc++6 glibc-source krb5-user libnss3 tini python3 python3-pip python3-setuptools \
    && rm -rf /var/lib/apt/lists/*

# Ensure Python3 is the default
RUN ln -s /usr/bin/python3 /usr/bin/python

# Download and install Apache Spark
RUN wget -O /tmp/spark-${SPARK_VERSION}-bin-without-hadoop.tgz \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz" && \
    tar -xzf /tmp/spark-${SPARK_VERSION}-bin-without-hadoop.tgz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-without-hadoop ${SPARK_HOME} && \
    rm -f /tmp/spark-${SPARK_VERSION}-bin-without-hadoop.tgz

# Download and install Hadoop
RUN wget -O /tmp/hadoop-${HADOOP_VERSION}.tar.gz \
    "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" && \
    tar -xzf /tmp/hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ && \
    ln -s /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm -f /tmp/hadoop-${HADOOP_VERSION}.tar.gz

# Set Spark classpath
ENV SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/yarn:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/tools/lib/*"

RUN mkdir /opt/app
COPY requirements.txt /opt/

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY app/start_app.sh /opt/app
RUN chmod a+x /opt/app/start_app.sh
RUN mkdir /opt/test

COPY app/*.py /opt/app/
COPY test/*.py /opt/test/