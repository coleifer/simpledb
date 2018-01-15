from miniredis import QueueServer, ProtocolHandler, Client

import contextlib
import threading
import time


def make_server():
    server = QueueServer(use_gevent=False)
    t = threading.Thread(target=server.run)
    t.daemon = True
    t.start()
    return server, t


@contextlib.contextmanager
def timed(s):
    start = time.time()
    yield
    duration = round(time.time() - start, 3)
    print('%s: %s' % (s, duration))


def run_benchmark(client):
    n = 10000
    with timed('get/set'):
        for i in range(n):
            client.set('k%d' % i, 'v%d' % i)

        for i in range(n + int(n * 0.1)):
            client.get('k%d' % i)

    with timed('serializing arrays'):
        arr = [1, 2, 3, 4, 5, 6, [7, 8, 9, [10, 11, 12], 13], 14, 15]
        for i in range(n):
            client.set('k%d' % i, arr)

        for i in range(n):
            client.get('k%d' % i)

    with timed('serializing dicts'):
        d = {'k1': 'v1', 'k2': 'v2', 'k3': {'v3': {'v4': 'v5'}}}
        for i in range(n):
            client.set('k%d' % i, d)

        for i in range(n):
            client.get('k%d' % i)


def main():
    server, t = make_server()
    time.sleep(0.1)

    client = Client()
    client.connect()

    try:
        run_benchmark(client)
    finally:
        client.shutdown()
        client.close()


if __name__ == '__main__':
    main()
