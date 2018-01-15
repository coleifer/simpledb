try:
    import gevent
    from gevent import socket
    from gevent.pool import Pool
    from gevent.server import StreamServer
except ImportError:
    import socket
    Pool = StreamServer = None

from collections import defaultdict
from collections import deque
from collections import namedtuple
from io import BytesIO
from socket import error as socket_error
import datetime
import heapq
import importlib
import json
import logging
import optparse
import sys
try:
    import socketserver as ss
except ImportError:
    import SocketServer as ss


__version__ = '0.1.0'

logger = logging.getLogger(__name__)


class ThreadedStreamServer(object):
    def __init__(self, address, handler):
        self.address = address
        self.handler = handler

    def serve_forever(self):
        handler = self.handler
        class RequestHandler(ss.BaseRequestHandler):
            def handle(self):
                return handler(self.request, self.client_address)

        class ThreadedServer(ss.ThreadingMixIn, ss.TCPServer):
            allow_reuse_address = True

        self.stream_server = ThreadedServer(self.address, RequestHandler)
        self.stream_server.serve_forever()


class CommandError(Exception):
    def __init__(self, message):
        self.message = message
        super(CommandError, self).__init__()


class Disconnect(Exception): pass


"""
Protocol is based on Redis wire protocol.

Client sends requests as an array of bulk strings.

Server replies, indicating response type using the first byte:

* "+" - simple string
* "-" - error
* ":" - integer
* "$" - bulk string
* "^" - bulk unicode string
* "@" - json string (uses bulk string rules)
* "*" - array
* "%" - dict

Simple strings: "+string content\r\n"  <-- cannot contain newlines

Error: "-Error message\r\n"

Integers: ":1337\r\n"

Bulk String: "$number of bytes\r\nstring data\r\n"

* Empty string: "$0\r\n\r\n"
* NULL: "$-1\r\n"

Array: "*number of elements\r\n...elements..."

* Empty array: "*0\r\n"

And a new data-type, dictionaries: "%number of elements\r\n...elements..."
"""
if sys.version_info[0] == 3:
    unicode = str
    basestring = (bytes, str)


def encode(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')
    elif isinstance(s, bytes):
        return s
    else:
        return str(s).encode('utf-8')


def decode(s):
    if isinstance(s, unicode):
        return s
    elif isinstance(s, bytes):
        return s.decode('utf-8')
    else:
        return str(s)


Error = namedtuple('Error', ('message',))


class ProtocolHandler(object):
    def __init__(self):
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_string,
            b'^': self.handle_unicode,
            b'@': self.handle_json,
            b'*': self.handle_array,
            b'%': self.handle_dict,
        }

    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip(b'\r\n')

    def handle_error(self, socket_file):
        return Error(socket_file.readline().rstrip(b'\r\n'))

    def handle_integer(self, socket_file):
        number = socket_file.readline().rstrip(b'\r\n')
        if b'.' in number:
            return float(number)
        return int(number)

    def handle_string(self, socket_file):
        length = int(socket_file.readline().rstrip(b'\r\n'))
        if length == -1:
            return None
        length += 2
        return socket_file.read(length)[:-2]

    def handle_unicode(self, socket_file):
        return self.handle_string(socket_file).decode('utf-8')

    def handle_json(self, socket_file):
        return json.loads(self.handle_string(socket_file))

    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip(b'\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]

    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip(b'\r\n'))
        elements = [self.handle_request(socket_file)
                    for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect()

        try:
            return self.handlers[first_byte](socket_file)
        except KeyError:
            rest = socket_file.readline().rstrip(b'\r\n')
            return first_byte + rest

    def write_response(self, socket_file, data):
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()

    def _write(self, buf, data):
        if isinstance(data, bytes):
            buf.write(b'$%d\r\n%s\r\n' % (len(data), data))
        elif isinstance(data, unicode):
            bdata = data.encode('utf-8')
            buf.write(b'^%d\r\n%s\r\n' % (len(bdata), bdata))
        elif data is True or data is False:
            buf.write(b':%d\r\n' % (1 if data else 0))
        elif isinstance(data, (int, float)):
            buf.write(b':%d\r\n' % data)
        elif isinstance(data, Error):
            buf.write(b'-%s\r\n' % bytes(data.message, 'utf-8'))
        elif isinstance(data, (list, tuple)):
            buf.write(b'*%d\r\n' % len(data))
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(b'%%%d\r\n' % len(data))
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            buf.write(b'$-1\r\n')


class Shutdown(Exception): pass


class QueueServer(object):
    def __init__(self, host='127.0.0.1', port=31337, max_clients=64,
                 use_gevent=True):
        self._host = host
        self._port = port
        self._max_clients = max_clients
        if use_gevent:
            if Pool is None or StreamServer is None:
                raise Exception('gevent not installed. Please install gevent '
                                'or instantiate QueueServer with '
                                'use_gevent=False')

            self._pool = Pool(max_clients)
            self._server = StreamServer(
                (self._host, self._port),
                self.connection_handler,
                spawn=self._pool)
        else:
            self._server = ThreadedStreamServer(
                (self._host, self._port),
                self.connection_handler)

        self._commands = self.get_commands()
        self._protocol = ProtocolHandler()

        self._hashes = defaultdict(dict)
        self._kv = {}
        self._queues = defaultdict(deque)
        self._schedule = []
        self._sets = defaultdict(set)

        self._active_connections = 0
        self._commands_processed = 0
        self._command_errors = 0
        self._connections = 0

    def get_commands(self):
        timestamp_re = (r'(?P<timestamp>\d{4}-\d{2}-\d{2} '
                        '\d{2}:\d{2}:\d{2}(?:\.\d+)?)')
        return dict((
            # Queue commands.
            (b'LPUSH', self.lpush),
            (b'RPUSH', self.rpush),
            (b'LPOP', self.lpop),
            (b'RPOP', self.rpop),
            (b'LREM', self.lrem),
            (b'LLEN', self.llen),
            (b'LINDEX', self.lindex),
            (b'LRANGE', self.lrange),
            (b'LSET', self.lset),
            (b'LTRIM', self.ltrim),
            (b'RPOPLPUSH', self.rpoplpush),
            (b'LFLUSH', self.lflush),

            # K/V commands.
            (b'APPEND', self.kv_append),
            (b'DECR', self.kv_decr),
            (b'DECRBY', self.kv_decrby),
            (b'DELETE', self.kv_delete),
            (b'EXISTS', self.kv_exists),
            (b'GET', self.kv_get),
            (b'GETSET', self.kv_getset),
            (b'INCR', self.kv_incr),
            (b'INCRBY', self.kv_incrby),
            (b'MGET', self.kv_mget),
            (b'MPOP', self.kv_mpop),
            (b'MSET', self.kv_mset),
            (b'POP', self.kv_pop),
            (b'SET', self.kv_set),
            (b'SETNX', self.kv_setnx),
            (b'LEN', self.kv_len),
            (b'FLUSH', self.kv_flush),

            # Hash commands.
            (b'HDEL', self.hdel),
            (b'HEXISTS', self.hexists),
            (b'HGET', self.hget),
            (b'HGETALL', self.hgetall),
            (b'HINCRBY', self.hincrby),
            (b'HKEYS', self.hkeys),
            (b'HLEN', self.hlen),
            (b'HMGET', self.hmget),
            (b'HMSET', self.hmset),
            (b'HSET', self.hset),
            (b'HSETNX', self.hsetnx),
            (b'HVALS', self.hvals),

            # Set commands.
            (b'SADD', self.sadd),
            (b'SCARD', self.scard),
            (b'SDIFF', self.sdiff),
            (b'SDIFFSTORE', self.sdiffstore),
            (b'SINTER', self.sinter),
            (b'SINTERSTORE', self.sinterstore),
            (b'SISMEMBER', self.sismember),
            (b'SMEMBERS', self.smembers),
            (b'SPOP', self.spop),
            (b'SREM', self.srem),
            (b'SUNION', self.sunion),
            (b'SUNIONSTORE', self.sunionstore),

            # Schedule commands.
            (b'ADD', self.schedule_add),
            (b'READ', self.schedule_read),
            (b'FLUSH_SCHEDULE', self.schedule_flush),
            (b'LENGTH_SCHEDULE', self.schedule_length),

            # Misc.
            (b'INFO', self.info),
            (b'FLUSHALL', self.flush_all),
            (b'SHUTDOWN', self.shutdown),
        ))

    def lpush(self, queue, *values):
        self._queues[queue].extendleft(values)
        return len(values)

    def rpush(self, queue, *values):
        self._queues[queue].extend(values)
        return len(values)

    def lpop(self, queue):
        try:
            return self._queues[queue].popleft()
        except IndexError:
            pass

    def rpop(self, queue):
        try:
            return self._queues[queue].pop()
        except IndexError:
            pass

    def lrem(self, queue, value):
        try:
            self._queues[queue].remove(value)
        except ValueError:
            return 0
        else:
            return 1

    def llen(self, queue):
        return len(self._queues[queue])

    def lindex(self, queue, idx):
        try:
            return self._queues[queue][idx]
        except IndexError:
            pass

    def lset(self, queue, idx, value):
        try:
            self._queues[queue][idx] = value
        except IndexError:
            return 0
        else:
            return 1

    def ltrim(self, queue, start, stop):
        self._queues[queue] = deque(list(self._queues[queue])[start:stop])

    def rpoplpush(self, src, dest):
        try:
            self._queues[dest].appendleft(self._queues[src].pop())
        except IndexError:
            return 0
        else:
            return 1

    def lrange(self, queue, start, end=None):
        return list(self._queues[queue])[start:end]

    def lflush(self, queue):
        qlen = self.llen(queue)
        self._queues[queue].clear()
        return qlen

    def kv_append(self, key, value):
        self._kv.setdefault(key, '')
        self._kv[key] += value
        return self._kv[key]

    def _kv_incr(self, key, n):
        self._kv.setdefault(key, 0)
        self._kv[key] += n
        return self._kv[key]

    def kv_decr(self, key):
        return self._kv_incr(key, -1)

    def kv_decrby(self, key, n):
        return self._kv_incr(key, -1 * n)

    def kv_delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0

    def kv_exists(self, key):
        return 1 if key in self._kv else 0

    def kv_get(self, key):
        return self._kv.get(key)

    def kv_getset(self, key, value):
        orig = self._kv.get(key)
        self._kv[key] = value
        return orig

    def kv_incr(self, key):
        return self._kv_incr(key, 1)

    def kv_incrby(self, key, n):
        return self._kv_incr(key, n)

    def kv_mget(self, *keys):
        return [self._kv.get(key) for key in keys]

    def kv_mpop(self, *keys):
        return [self._kv.pop(key, None) for key in keys]

    def kv_mset(self, *items):
        for idx in range(0, len(items), 2):
            self._kv[items[idx]] = items[idx + 1]
        return len(items) / 2

    def kv_pop(self, key):
        return self._kv.pop(key, None)

    def kv_set(self, key, value):
        self._kv[key] = value
        return 1

    def kv_setnx(self, key, value):
        if key in self._kv:
            return 0
        else:
            self._kv[key] = value
            return 1

    def kv_len(self):
        return len(self._kv)

    def kv_flush(self):
        kvlen = self.kv_length()
        self._kv.clear()
        return kvlen

    def _decode_timestamp(self, timestamp):
        fmt = '%Y-%m-%d %H:%M:%S'
        if b'.' in timestamp:
            fmt = fmt + '.%f'
        try:
            return datetime.datetime.strptime(decode(timestamp), fmt)
        except ValueError:
            raise CommandError('Timestamp must be formatted Y-m-d H:M:S')

    def hdel(self, key, field):
        if self.hexists(key, field):
            del self._hashes[key][field]
            return 1
        return 0

    def hexists(self, key, field):
        return 1 if field in self._hashes[key] else 0

    def hget(self, key, field):
        return self._hashes[key].get(field)

    def hgetall(self, key):
        return self._hashes[key]

    def hincrby(self, key, field, incr=1):
        self._hashes[key].setdefault(field, 0)
        self._hashes[key][field] += incr
        return self._hashes[key][field]

    def hkeys(self, key):
        return list(self._hashes[key])

    def hlen(self, key):
        return len(self._hashes[key])

    def hmget(self, key, *fields):
        accum = {}
        for field in fields:
            accum[field] = self._hashes[key].get(field)
        return accum

    def hmset(self, key, data):
        self._hashes[key].update(data)
        return len(data)

    def hset(self, key, field, value):
        self._hashes[key][field] = value
        return 1

    def hsetnx(self, key, field, value):
        if field not in self._hashes[key]:
            self._hashes[key][field] = value
            return 1
        return 0

    def hvals(self, key):
        return list(self._hashes[key].values())

    def sadd(self, key, *members):
        self._sets[key].update(members)
        return len(self._sets[key])

    def scard(self, key):
        return len(self._sets[key])

    def sdiff(self, key, *keys):
        src = set(self._sets[key])
        for key in keys:
            src -= self._sets[key]
        return list(src)

    def sdiffstore(self, dest, key, *keys):
        src = set(self._sets[key])
        for key in keys:
            src -= self._sets[key]
        self._sets[dest] = src
        return len(src)

    def sinter(self, key, *keys):
        src = set(self._sets[key])
        for key in keys:
            src &= self._sets[key]
        return list(src)

    def sinterstore(self, dest, key, *keys):
        src = set(self._sets[key])
        for key in keys:
            src &= self._sets[key]
        self._sets[dest] = src
        return len(src)

    def sismember(self, key, member):
        return 1 if member in self._sets[key] else 0

    def smembers(self, key):
        return list(self._sets[key])

    def spop(self, key, n=1):
        accum = []
        for _ in range(n):
            try:
                accum.append(self._sets[key].pop())
            except KeyError:
                break
        return accum

    def srem(self, key, *members):
        ct = 0
        for member in members:
            try:
                self._sets[key].remove(member)
            except KeyError:
                pass
            else:
                ct += 1
        return ct

    def sunion(self, key, *keys):
        src = set(self._sets[key])
        for key in keys:
            src |= self._sets[key]
        return list(src)

    def sunionstore(self, dest, key, *keys):
        src = set(self._sets[key])
        for key in keys:
            src |= self._sets[key]
        self._sets[dest] = src
        return len(src)

    def schedule_add(self, timestamp, data):
        dt = self._decode_timestamp(timestamp)
        heapq.heappush(self._schedule, (dt, data))
        return 1

    def schedule_read(self, timestamp=None):
        dt = self._decode_timestamp(timestamp)
        accum = []
        while self._schedule and self._schedule[0][0] <= dt:
            ts, data = heapq.heappop(self._schedule)
            accum.append(data)
        return accum

    def schedule_flush(self):
        schedulelen = self.schedule_length()
        self._schedule = []
        return schedulelen

    def schedule_length(self):
        return len(self._schedule)

    def info(self):
        return {
            'active_connections': self._active_connections,
            'commands_processed': self._commands_processed,
            'command_errors': self._command_errors,
            'connections': self._connections}

    def flush_all(self):
        self._hashes = defaultdict(dict)
        self._queues = defaultdict(deque)
        self._sets = defaultdict(set)
        self.kv_flush()
        self.schedule_flush()
        return 1

    def shutdown(self):
        raise Shutdown('shutting down')

    def run(self):
        self._server.serve_forever()

    def connection_handler(self, conn, address):
        logger.info('Connection received: %s:%s' % address)
        socket_file = conn.makefile('rwb')
        self._active_connections += 1
        while True:
            try:
                self.request_response(socket_file)
            except Disconnect:
                logger.info('Client went away: %s:%s' % address)
                break
            except Exception as exc:
                logger.exception('Error processing command.')
        self._active_connections -= 1

    def request_response(self, socket_file):
        data = self._protocol.handle_request(socket_file)

        try:
            resp = self.respond(data)
        except Shutdown:
            logger.info('Shutting down')
            self._protocol.write_response(socket_file, 1)
            raise KeyboardInterrupt()
        except CommandError as command_error:
            resp = Error(command_error.message)
            self._command_errors += 1
        except Exception as exc:
            logger.exception('Unhandled error')
            resp = Error('Unhandled server error')
        else:
            self._commands_processed += 1

        self._protocol.write_response(socket_file, resp)

    def respond(self, data):
        if not isinstance(data, list):
            try:
                data = data.split()
            except:
                raise CommandError('Unrecognized request type.')

        if not isinstance(data[0], basestring):
            raise CommandError('First parameter must be command name.')

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError('Unrecognized command: %s' % command)
        else:
            logger.debug('Received %s', decode(command))

        return self._commands[command](*data[1:])


class Client(object):
    def __init__(self, host='127.0.0.1', port=31337):
        self._host = host
        self._port = port
        self._socket = None
        self._fh = None
        self._protocol = ProtocolHandler()
        self._is_closed = True

    def connect(self):
        if not self._is_closed:
            self.close()

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self._host, self._port))
        self._fh = self._socket.makefile('rwb')
        self._is_closed = False
        return True

    def close(self):
        if self._is_closed:
            return False

        self._socket.close()
        self._is_closed = True
        return True

    def execute(self, *args):
        if self._is_closed:
            self.connect()
        self._protocol.write_response(self._fh, args)
        resp = self._protocol.handle_request(self._fh)
        if isinstance(resp, Error):
            raise CommandError(resp.message)
        return resp

    def command(cmd):
        def method(self, *args):
            return self.execute(cmd.encode('utf-8'), *args)
        return method

    lpush = command('LPUSH')
    rpush = command('RPUSH')
    lpop = command('LPOP')
    rpop = command('RPOP')
    lrem = command('LREM')
    llen = command('LLEN')
    lindex = command('LINDEX')
    lrange = command('LRANGE')
    lset = command('LSET')
    ltrim = command('LTRIM')
    rpoplpush = command('RPOPLPUSH')
    lflush = command('LFLUSH')

    append = command('APPEND')
    decr = command('DECR')
    decrby = command('DECRBY')
    delete = command('DELETE')
    exists = command('EXISTS')
    get = command('GET')
    getset = command('GETSET')
    incr = command('INCR')
    incrby = command('INCRBY')
    mget = command('MGET')
    mpop = command('MPOP')
    mset = command('MSET')
    pop = command('POP')
    set = command('SET')
    setnx = command('SETNX')
    length = command('LEN')
    flush = command('FLUSH')

    hdel = command('HDEL')
    hexists = command('HEXISTS')
    hget = command('HGET')
    hgetall = command('HGETALL')
    hincrby = command('HINCRBY')
    hkeys = command('HKEYS')
    hlen = command('HLEN')
    hmget = command('HMGET')
    hmset = command('HMSET')
    hset = command('HSET')
    hsetnx = command('HSETNX')
    hvals = command('HVALS')

    sadd = command('SADD')
    scard = command('SCARD')
    sdiff = command('SDIFF')
    sdiffstore = command('SDIFFSTORE')
    sinter = command('SINTER')
    sinterstore = command('SINTERSTORE')
    sismember = command('SISMEMBER')
    smembers = command('SMEMBERS')
    spop = command('SPOP')
    srem = command('SREM')
    sunion = command('SUNION')
    sunionstore = command('SUNIONSTORE')

    add = command('ADD')
    read = command('READ')
    flush_schedule = command('FLUSH_SCHEDULE')
    length_schedule = command('LENGTH_SCHEDULE')

    flushall = command('FLUSHALL')
    shutdown = command('SHUTDOWN')


def get_option_parser():
    parser = optparse.OptionParser()
    parser.add_option('-d', '--debug', action='store_true', dest='debug',
                      help='Log debug messages.')
    parser.add_option('-e', '--errors', action='store_true', dest='error',
                      help='Log error messages only.')
    parser.add_option('-t', '--use-threads', action='store_false',
                      default=True, dest='use_gevent',
                      help='Use threads instead of gevent.')
    parser.add_option('-H', '--host', default='127.0.0.1', dest='host',
                      help='Host to listen on.')
    parser.add_option('-m', '--max-clients', default=64, dest='max_clients',
                      help='Maximum number of clients.', type=int)
    parser.add_option('-p', '--port', default=31337, dest='port',
                      help='Port to listen on.', type=int)
    parser.add_option('-l', '--log-file', dest='log_file', help='Log file.')
    parser.add_option('-x', '--extension', action='append', dest='extensions',
                      help='Import path for Python extension module(s).')
    return parser


def configure_logger(options):
    logger.addHandler(logging.StreamHandler())
    if options.log_file:
        logger.addHandler(logging.FileHandler(options.log_file))
    if options.debug:
        logger.setLevel(logging.DEBUG)
    elif options.error:
        logger.setLevel(logging.ERROR)
    else:
        logger.setLevel(logging.INFO)


def load_extensions(server, extensions):
    for extension in extensions:
        try:
            module = importlib.import_module(extension)
        except ImportError:
            logger.exception('Could not import extension %s' % extension)
        else:
            try:
                initialize = getattr(module, 'initialize')
            except AttributeError:
                logger.exception('Could not find "initialize" function in '
                                 'extension %s' % extension)
                raise
            else:
                initialize(server)
                logger.info('Loaded %s extension.' % extension)


if __name__ == '__main__':
    options, args = get_option_parser().parse_args()

    if options.use_gevent:
        from gevent import monkey; monkey.patch_all()

    configure_logger(options)
    server = QueueServer(host=options.host, port=options.port,
                         max_clients=options.max_clients,
                         use_gevent=options.use_gevent)
    load_extensions(server, options.extensions or ())
    server.run()
