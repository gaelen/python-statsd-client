# -*- coding: utf-8 -*-
#
# This file is part of python-statsd-client released under the Apache
# License, Version 2.0. See the NOTICE for more information.

import unittest
import socket
import time
import statsd


class mock_udp_socket(object):
    def __init__(self, family, socktype):
        assert family == socket.AF_INET
        assert socktype == socket.SOCK_DGRAM

    def sendto(self, data, addr):
        self.data = data


class TestStatsd(unittest.TestCase):

    def setUp(self):
        # Moneky patch statsd socket for testing
        statsd.socket = mock_udp_socket
        statsd.init_statsd()

    def tearDown(self):
        statsd.STATSD_HOST = 'localhost'
        statsd.STATSD_PORT = 8125
        statsd.STATSD_SAMPLE_RATE = None
        statsd.STATSD_BUCKET_PREFIX = None

    def test_init_statsd(self):
        settings = {'STATSD_HOST': '127.0.0.1',
                    'STATSD_PORT': 9999,
                    'STATSD_SAMPLE_RATE': 0.99,
                    'STATSD_BUCKET_PREFIX': 'testing'}
        statsd.init_statsd(settings)
        self.assertEqual(statsd.STATSD_HOST, '127.0.0.1')
        self.assertEqual(statsd.STATSD_PORT, 9999)
        self.assertEqual(statsd.STATSD_SAMPLE_RATE, 0.99)
        self.assertEqual(statsd.STATSD_BUCKET_PREFIX, 'testing')

    def test_decrement(self):
        statsd.decrement('counted')
        self.assertEqual(statsd._statsd._socket.data, b'counted:-1|c')
        statsd.decrement('counted', 5)
        self.assertEqual(statsd._statsd._socket.data, b'counted:-5|c')
        statsd.decrement('counted', 5, 0.99)
        self.assertTrue(statsd._statsd._socket.data.startswith(b'counted:-5|c'))
        if statsd._statsd._socket.data != b'counted:-5|c':
            self.assertTrue(statsd._statsd._socket.data.endswith(b'|@0.99'))

    def test_increment(self):
        statsd.increment('counted')
        self.assertEqual(statsd._statsd._socket.data, b'counted:1|c')
        statsd.increment('counted', 5)
        self.assertEqual(statsd._statsd._socket.data, b'counted:5|c')
        statsd.increment('counted', 5, 0.99)
        self.assertTrue(statsd._statsd._socket.data.startswith(b'counted:5|c'))
        if statsd._statsd._socket.data != b'counted:5|c':
            self.assertTrue(statsd._statsd._socket.data.endswith(b'|@0.99'))

    def test_gauge(self):
        statsd.gauge('gauged', 1)
        self.assertEqual(statsd._statsd._socket.data, b'gauged:1|g')
        statsd.gauge('gauged', 5)
        self.assertEqual(statsd._statsd._socket.data, b'gauged:5|g')
        statsd.gauge('gauged', -5, 0.99)
        self.assertTrue(statsd._statsd._socket.data.startswith(b'gauged:-5|g'))
        if statsd._statsd._socket.data != b'gauged:-5|g':
            self.assertTrue(statsd._statsd._socket.data.endswith(b'|@0.99'))

    def test_timing(self):
        statsd.timing('timed', 250)
        self.assertEqual(statsd._statsd._socket.data, b'timed:250|ms')
        statsd.timing('timed', 250, 0.99)
        self.assertTrue(statsd._statsd._socket.data.startswith(b'timed:250|ms'))
        if statsd._statsd._socket.data != b'timed:250|ms':
            self.assertTrue(statsd._statsd._socket.data.endswith(b'|@0.99'))


class TestStatsdClient(unittest.TestCase):

    def setUp(self):
        # Moneky patch statsd socket for testing
        statsd.socket = mock_udp_socket

    def test_prefix(self):
        client = statsd.StatsdClient('localhost', 8125, prefix='main.bucket', sample_rate=None)
        client._send(b'subname', b'100|c')
        self.assertEqual(client._socket.data, b'main.bucket.subname:100|c')

        client = statsd.StatsdClient('localhost', 8125, prefix='main', sample_rate=None)
        client._send(b'subname', b'100|c')
        self.assertEqual(client._socket.data, b'main.subname:100|c')
        client._send(b'subname.subsubname', b'100|c')
        self.assertEqual(client._socket.data, b'main.subname.subsubname:100|c')

    def test_decr(self):
        client = statsd.StatsdClient('localhost', 8125, prefix='', sample_rate=None)
        client.decr('buck.counter', 5)
        self.assertEqual(client._socket.data, b'buck.counter:-5|c')

    def test_decr_sample_rate(self):
        client = statsd.StatsdClient('localhost', 8125, prefix='', sample_rate=0.999)
        client.decr('buck.counter', 5)
        self.assertEqual(client._socket.data, b'buck.counter:-5|c|@0.999')
        if client._socket.data != 'buck.counter:-5|c':
            self.assertTrue(client._socket.data.endswith(b'|@0.999'))

    def test_incr(self):
        client = statsd.StatsdClient('localhost', 8125, prefix='', sample_rate=None)
        client.incr('buck.counter', 5)
        self.assertEqual(client._socket.data, b'buck.counter:5|c')

    def test_incr_sample_rate(self):
        client = statsd.StatsdClient('localhost', 8125, prefix='', sample_rate=0.999)
        client.incr('buck.counter', 5)
        self.assertEqual(client._socket.data, b'buck.counter:5|c|@0.999')
        if client._socket.data != 'buck.counter:5|c':
            self.assertTrue(client._socket.data.endswith(b'|@0.999'))

    def test_send(self):
        client = statsd.StatsdClient('localhost', 8125, prefix='', sample_rate=None)
        client._send(b'buck', b'50|c')
        self.assertEqual(client._socket.data, b'buck:50|c')

    def test_send_sample_rate(self):
        client = statsd.StatsdClient('localhost', 8125, prefix='', sample_rate=0.999)
        client._send(b'buck', b'50|c')
        self.assertEqual(client._socket.data, b'buck:50|c|@0.999')
        if client._socket.data != 'buck:50|c':
            self.assertTrue(client._socket.data.endswith(b'|@0.999'))

    def test_timing(self):
        client = statsd.StatsdClient('localhost', 8125, prefix='', sample_rate=None)
        client.timing('buck.timing', 100)
        self.assertEqual(client._socket.data, b'buck.timing:100|ms')

    def test_timing_sample_rate(self):
        client = statsd.StatsdClient('localhost', 8125, prefix='', sample_rate=0.999)
        client.timing('buck.timing', 100)
        self.assertEqual(client._socket.data, b'buck.timing:100|ms|@0.999')
        if client._socket.data != '':
            self.assertTrue(client._socket.data.endswith(b'|@0.999'))


class TestStatsdCounter(unittest.TestCase):

    def setUp(self):
        # Moneky patch statsd socket for testing
        statsd.socket = mock_udp_socket

    def test_add(self):
        counter = statsd.StatsdCounter('counted', 'localhost', 8125, prefix='', sample_rate=None)
        counter += 1
        self.assertEqual(counter._client._socket.data, b'counted:1|c')
        counter += 5
        self.assertEqual(counter._client._socket.data, b'counted:5|c')

    def test_sub(self):
        counter = statsd.StatsdCounter('counted', 'localhost', 8125, prefix='', sample_rate=None)
        counter -= 1
        self.assertEqual(counter._client._socket.data, b'counted:-1|c')
        counter -= 5
        self.assertEqual(counter._client._socket.data, b'counted:-5|c')


class TestStatsdTimer(unittest.TestCase):

    def setUp(self):
        # Moneky patch statsd socket for testing
        statsd.socket = mock_udp_socket

    def test_startstop(self):
        timer = statsd.StatsdTimer('timeit', 'localhost', 8125, prefix='', sample_rate=None)
        timer.start()
        time.sleep(0.25)
        timer.stop()
        self.assertTrue(timer._client._socket.data.startswith(b'timeit.total:2'))
        self.assertTrue(timer._client._socket.data.endswith(b'|ms'))

    def test_split(self):
        timer = statsd.StatsdTimer('timeit', 'localhost', 8125, prefix='', sample_rate=None)
        timer.start()
        time.sleep(0.25)
        timer.split('lap')
        self.assertTrue(timer._client._socket.data.startswith(b'timeit.lap:2'))
        self.assertTrue(timer._client._socket.data.endswith(b'|ms'))
        time.sleep(0.26)
        timer.stop()
        self.assertTrue(timer._client._socket.data.startswith(b'timeit.total:5'))
        self.assertTrue(timer._client._socket.data.endswith(b'|ms'))

    def test_wrap(self):
        class TC(object):
            @statsd.StatsdTimer.wrap('timeit')
            def do(self):
                time.sleep(0.25)
                return 1
        tc = TC()
        result = tc.do()
        self.assertEqual(result, 1)

    def test_with(self):
        timer = statsd.StatsdTimer('timeit', 'localhost', 8125, prefix='', sample_rate=None)
        with timer:
            time.sleep(0.25)
        self.assertTrue(timer._client._socket.data.startswith(b'timeit.total:2'))
        self.assertTrue(timer._client._socket.data.endswith(b'|ms'))

if __name__ == '__main__':
    unittest.main()
