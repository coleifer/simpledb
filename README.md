### simpledb

Miniature [Redis](https://redis.io)-like database written in Python.

#### installing

```
$ pip install simpledb
```

Alternatively, you can install from git:

```
$ git clone https://github.com/coleifer/simpledb
$ cd simpledb
$ python setup.py install
```

#### running

by default, the simpledb server runs on localhost:31337.

the following options are supported:

```
Usage: simpledb.py [options]

Options:
  -h, --help            show this help message and exit
  -d, --debug           Log debug messages.
  -e, --errors          Log error messages only.
  -t, --use-threads     Use threads instead of gevent.
  -H HOST, --host=HOST  Host to listen on.
  -m MAX_CLIENTS, --max-clients=MAX_CLIENTS
                        Maximum number of clients.
  -p PORT, --port=PORT  Port to listen on.
  -l LOG_FILE, --log-file=LOG_FILE
                        Log file.
  -x EXTENSIONS, --extension=EXTENSIONS
                        Import path for Python extension module(s).
```

to run with debug logging on port 31339, for example:

```
$ simpledb.py -d -p 31339
```

#### docker

simpledb ships with a [Dockerfile](https://github.com/coleifer/simpledb/blob/master/docker/Dockerfile)
or can be pulled from dockerhub as *coleifer/simpledb*. The dockerfile setups
up a volume at `/var/lib/simpledb` and exposes port `31337`.

running:

```console

$ docker run -it --rm -p 31337:31337 coleifer/simpledb
```

building:

```console

$ cd simpledb/docker
$ docker build -t simpledb .
$ docker run -d -p 31337:31337 -v simpledb-logs:/var/lib/simpledb simpledb
```

#### usage

the server is capable of storing the following data-types natively:

* strings and/or binary data
* numerical values
* null
* lists (may be nested)
* dictionaries (may be nested)

```python

from simpledb import Client

client = Client()
client.set('key', {'name': 'Charlie', 'pets': ['mickey', 'huey']})

print(client.get('key'))
{'name': 'Charlie', 'pets': ['mickey', 'huey']}
```
