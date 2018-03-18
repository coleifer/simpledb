FROM python:3.6

WORKDIR /tmp
RUN pip install gevent simpledb
EXPOSE 31337
VOLUME /var/lib/simpledb
ENTRYPOINT ["simpledb.py", "-l", "/var/lib/simpledb/server.log", "-H", "0.0.0.0"]
