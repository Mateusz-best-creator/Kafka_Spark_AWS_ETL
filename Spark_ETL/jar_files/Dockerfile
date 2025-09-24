FROM ubuntu:22.04

RUN mkdir /app

WORKDIR /app

RUN apt-get -y update && \ 
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY download_jar.sh /app/download_jar.sh

RUN chmod a+x /app/download_jar.sh

ENTRYPOINT ["./download_jar.sh"]