FROM alpine:3.7

WORKDIR /tmp
RUN apk add --no-cache --virtual .build-deps build-base python3-dev \
      && apk add --no-cache libev python3 py3-pip \
      && pip3 install --no-cache-dir gevent simpledb \
      && apk del .build-deps
EXPOSE 31337
VOLUME /var/lib/simpledb
ENTRYPOINT ["simpledb.py", "-l", "/var/lib/simpledb/server.log", "-H", "0.0.0.0"]
