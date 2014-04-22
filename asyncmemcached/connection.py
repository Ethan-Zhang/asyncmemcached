#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2012 Ethan Zhang<http://github.com/Ethan-Zhang> 
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.


import sys
import time
from socket import socket, AF_INET, SOCK_STREAM, error
import logging
import functools
import contextlib

from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
from tornado import stack_context

from error import ConnectionError, RedisError

class Connection(object):

    def __init__(self, host='localhost', port=11211, pool=None):
        self._host = host
        self._port = port
        self._pool = pool
        self._socket = None
        self._stream = None
        self._ioloop = IOLoop.instance()
        self.connect()

    def connect(self):
        try:       
            self._socket = socket(AF_INET, SOCK_STREAM, 0)
            self._socket.connect((self._host, self._port))
            self._stream = IOStream(self._socket, io_loop=self._ioloop)
            self._stream.set_close_callback(self.on_disconnect)
        except error as e:
            raise ConnectionError(e)

    def disconect(self):
        self._stream.close()

    def on_disconnect(self):
        if self._final_callback:
            self._final_callback(None)
            logging.warning('connection closed.')
            self._final_callback = None

    def send_command(self, fullcmd, expect_str, callback):
        self._final_callback = callback
        fullcmd = fullcmd + '\r\n'
        with stack_context.StackContext(self.cleanup):
            if fullcmd[0:3] == 'get' or \
                    fullcmd[0:4] == 'incr' or \
                    fullcmd[0:4] == 'decr':
                self._stream.write(fullcmd, self.read_value)
            else:
                self._stream.write(fullcmd,
                        functools.partial(self.read_response, expect_str))
    
    def read_response(self, expect_str):
        self._stream.read_until('\r\n', 
                        functools.partial(self._expect_callback,
                                        expect_str))
    def read_value(self):
        self._stream.read_until('\r\n', self._expect_value_header_callback)

    def _expect_value_header_callback(self, response):

        response = response[:-2]

        if response[:5] == 'VALUE':
            resp, key, flag, length = response.split()
            flag = int(flag)
            length = int(length)
            self._stream.read_bytes(length+2, self._expect_value_callback)
        elif response.isdigit():
            self._pool.release(self)
            if self._final_callback:
                self._final_callback(int(response))
                self._final_callback = None
        else:
            self._pool.release(self)
            if self._final_callback:
                self._final_callback(None)
                self._final_callback = None

    def _expect_value_callback(self, value):
        
        value = value[:-2]
        self._stream.read_until('\r\n',
                functools.partial(self._end_value_callback, value))

    def _end_value_callback(self, value, response):
        self._pool.release(self)
        response = response.rstrip('\r\n')

        if response == 'END':
            if self._final_callback:
                self._final_callback(value)
                self._final_callback = None
        else:
            raise RedisError('error %s' % response)

    def _expect_callback(self, expect_str, response):
        self._pool.release(self)
        response = response.rstrip('\r\n')

        if response == expect_str:
            if self._final_callback:
                self._final_callback(None)
                self._final_callback = None
        else:
            raise RedisError('error %s' % response)

    @contextlib.contextmanager
    def cleanup(self):
        try:
            yield
        except Exception as e:
            logging.warning("uncaught exception", exc_info=True)
            if self._final_callback:
                self._final_callback(None)
                self._final_callback = None
