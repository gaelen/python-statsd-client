
from __future__ import absolute_import
import random
from socket import socket, AF_INET, SOCK_DGRAM
import time

__version__ = '1.0'

STATSD_HOST = 'localhost'
STATSD_PORT = 8125
STATSD_SAMPLE_RATE = None
STATSD_BUCKET_PREFIX = None


def decrement(bucket, delta=1, sample_rate=None):
    _statsd.decr(bucket, delta, sample_rate)


def increment(bucket, delta=1, sample_rate=None):
    _statsd.incr(bucket, delta, sample_rate)


def timing(bucket, ms, sample_rate=None):
    _statsd.timing(bucket, ms, sample_rate)


class StatsdClient(object):

    def __init__(self, host=None, port=None, prefix=None, sample_rate=None):
        self._host = host or STATSD_HOST
        self._port = port or STATSD_PORT
        self._sample_rate = sample_rate or STATSD_SAMPLE_RATE
        self._prefix = prefix or STATSD_BUCKET_PREFIX
        self._socket = socket(AF_INET, SOCK_DGRAM)

    def decr(self, bucket, delta=1, sample_rate=None):
        """Decrements a counter by delta.
        """
        value = b'%d|c' % (-1 * delta)
        self._send(bucket, value, sample_rate)

    def incr(self, bucket, delta=1, sample_rate=None):
        """Increment a counter by delta.
        """
        value = b'%d|c' % delta
        self._send(bucket, value, sample_rate)

    def _send(self, bucket, value, sample_rate=None):
        """Format and send data to statsd.
        """
        sample_rate = sample_rate or self._sample_rate

        if sample_rate and sample_rate < 1.0 and sample_rate > 0:
            if random.random() <= sample_rate:
                value = b'%s|@%s' % (value, sample_rate)
        stat = b'%s:%s' % (bucket, value)
        if self._prefix:
            stat = self._prefix + b'.' + stat

        self._socket.sendto(stat, (self._host, self._port))

    def timing(self, bucket, ms, sample_rate=None):
        """Creates a timing sample.
        """
        value = b'%d|ms' % ms
        self._send(bucket, value, sample_rate)


class StatsdCounter(object):
    """Counter for StatsD.
    """
    def __init__(self, bucket, host=None, port=None, prefix=None,
                 sample_rate=None):
        self._client = StatsdClient(host=host, port=port, prefix=prefix,
                                    sample_rate=sample_rate)
        self._bucket = bucket

    def __add__(self, num):
        self._client.incr(self._bucket, delta=num)
        return self

    def __sub__(self, num):
        self._client.decr(self._bucket, delta=num)
        return self


class StatsdTimer(object):
    """Timer for StatsD.
    """
    def __init__(self, bucket, host=None, port=None, prefix=None,
                 sample_rate=None):
        self._client = StatsdClient(host=host, port=port, prefix=prefix,
                                    sample_rate=sample_rate)
        self._bucket = bucket

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        if type is not None:
            self.stop('total-except')
        else:
            self.stop()

    def start(self, bucket_key='start'):
        """Start the timer.
        """
        self._start = time.time() * 1000
        self._splits = [(bucket_key, self._start), ]

    def split(self, bucket_key):
        """Records time since start() or last call to split() and sends
        result to statsd.
        """
        self._splits.append((bucket_key, time.time() * 1000))
        self._client.timing(self._bucket + '.' + bucket_key,
                            self._splits[-1][1] - self._splits[-2][1])

    def stop(self, bucket_key='total'):
        """Stops the timer and sends total time to statsd.
        """
        self._stop = time.time() * 1000
        self._client.timing(self._bucket + '.' + bucket_key,
                            self._stop - self._start)

    @staticmethod
    def wrap(bucket):
        def wrapper(func):
            def f(*args, **kw):
                with StatsdTimer(bucket):
                    func(*args, **kw)
            return f
        return wrapper


def init_statsd(settings=None):
    """Initialize global statsd client.
    """
    global _statsd
    global STATSD_HOST
    global STATSD_PORT
    global STATSD_SAMPLE_RATE
    global STATSD_BUCKET_PREFIX

    if settings:
        STATSD_HOST = settings.get('STATSD_HOST', STATSD_HOST)
        STATSD_PORT = settings.get('STATSD_PORT', STATSD_PORT)
        STATSD_SAMPLE_RATE = settings.get('STATSD_SAMPLE_RATE',
                                          STATSD_SAMPLE_RATE)
        STATSD_BUCKET_PREFIX = settings.get('STATSD_BUCKET_PREFIX',
                                            STATSD_BUCKET_PREFIX)
    _statsd = StatsdClient(STATSD_HOST, STATSD_PORT,
                           STATSD_SAMPLE_RATE, STATSD_BUCKET_PREFIX)
    return _statsd

_statsd = init_statsd()
