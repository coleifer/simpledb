cdef bytes encode(s):
    if isinstance(s, bytes):
        return s
    elif isinstance(s, unicode):
        return s.encode('utf-8')
    else:
        return str(s).encode('utf-8')


cdef unicode decode(b):
    if isinstance(b, unicode):
        return b
    elif isinstance(b, bytes):
        return b.decode('utf-8')
    else:
        return str(b)


cdef class ProtocolHandler(object):
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

    cdef handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip(b'\r\n')

    cdef handle_error(self, socket_file):
        return Error(socket_file.readline().rstrip(b'\r\n'))

    cdef handle_integer(self, socket_file):
        number = socket_file.readline().rstrip(b'\r\n')
        if b'.' in number:
            return float(number)
        return int(number)

    cdef handle_string(self, socket_file):
        length = int(socket_file.readline().rstrip(b'\r\n'))
        if length == -1:
            return None
        length += 2
        return socket_file.read(length)[:-2]

    cdef handle_unicode(self, socket_file):
        return self.handle_string(socket_file).decode('utf-8')

    cdef handle_json(self, socket_file):
        return json.loads(self.handle_string(socket_file))

    cdef handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip(b'\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]

    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip(b'\r\n'))
        elements = [self.handle_request(socket_file)
                    for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    cdef handle_request(self, socket_file):
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

    cdef _write(self, buf, data):
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
