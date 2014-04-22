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


import logging

from pool import ConnectionPool


class Client(object):
    """
    Client connection to represent a remote database.

    Internally Client maintains a pool of connections that will live beyond the life of this object.

    :Parameters:
      - '**kwargs': passed to 'pool.ConnectionPool'
          - 'mincached' (optional): minimum connections to open on instantiation. 0 to open connections on first use
          - 'maxcached' (optionsal): maximum inactive cached connections for this pool. 0 for unlimited
          - 'maxconnections' (optional): maximum open connections for this pool. 0 for unlimited

    @return a 'Client' instance that wraps a 'pool.ConnetcionPool'

    Usage:
        >> db = asyncmemcached.Client(addr_list=['127.0.0.1:12345'])

    """

    def __init__(self, host='localhost', port=11211, connection_pool=None,
            mincached=0, maxcached=0, maxconnections=0):

        if not connection_pool:
            kwargs = {
                    'host':host,
                    'port':port,
                    'mincached':mincached,
                    'maxcached':maxcached,
                    'maxconnections':maxconnections,
                    }
            connection_pool = ConnectionPool(**kwargs)
        self._pool = connection_pool

    def add(self, key, value, flag=0, expired=0, callback=None):

        con = self._pool.get_connection()
        cmd = 'add %s %s %s %s\r\n%s' % (key, flag, expired, len(value), value)
        con.send_command(cmd, 'STORED', callback)

    def replace(self, key, value, flag=0, expired=0, callback=None):

        con = self._pool.get_connection()
        cmd = 'replace %s %s %s %s\r\n%s' % (key, flag, expired, len(value), value)
        con.send_command(cmd, 'STORED', callback)

    def set(self, key, value, flag=0, expired=0, callback=None):

        con = self._pool.get_connection()
        cmd = 'set %s %s %s %s\r\n%s' % (key, flag, expired, len(value), value)
        con.send_command(cmd, 'STORED', callback)

    def incr(self, key, delta=1, callback=None):
        con = self._pool.get_connection()
        cmd = 'incr %s %s' % (key, delta)
        con.send_command(cmd, '', callback)

    def decr(self, key, delta=1, callback=None):

        con = self._pool.get_connection()
        cmd = 'decr %s %s' % (key, delta)
        con.send_command(cmd, '', callback)

    def _incr_callback(self, connection, callback):
        
        connection.read_value(callback)
        
    def delete(self, key, callback=None):

        con = self._pool.get_connection()
        cmd = 'delete %s\r\n' % key
        con.send_command(cmd, 'DELETED', callback)

    def get(self, key, callback=None):
        cmd = 'get %s\r\n' % key
        con = self._pool.get_connection()
        con.send_command(cmd, '', callback)

