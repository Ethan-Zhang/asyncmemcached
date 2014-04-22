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

from threading import Condition
from connection import Connection

class ConnectionPool(object):
    """Connection Pool to a single memcached instance.
    
    :Parameters:
      - `mincached` (optional): minimum connections to open on instantiation. 0 to open connections on first use
      - `maxcached` (optional): maximum inactive cached connections for this pool. 0 for unlimited
      - `maxconnections` (optional): maximum open connections for this pool. 0 for unlimited
      - `**kwargs`: passed to `connection.Connection`
    
    """
    def __init__(self, 
                mincached=0, 
                maxcached=0, 
                maxconnections=0, 
                *args, **kwargs):
        assert isinstance(mincached, int)
        assert isinstance(maxcached, int)
        assert isinstance(maxconnections, int)
        if mincached and maxcached:
            assert mincached <= maxcached
        if maxconnections:
            assert maxconnections >= maxcached
            assert maxconnections >= mincached
        self._args, self._kwargs = args, kwargs
        self._mincached = mincached
        self._maxcached = maxcached
        self._maxconnections = maxconnections
        self._idle_cache = [] # the actual connections that can be used
        self._condition = Condition()
        self._connections = 0

        
        # Establish an initial number of idle database connections:
        idle = [self.connection() for i in range(mincached)]
        while idle:
            self.cache(idle.pop())
    
    def make_connection(self):
        kwargs = self._kwargs
        kwargs['pool'] = self
        return Connection(*self._args, **kwargs)
    
    def get_connection(self):
        """ get a cached connection from the pool """
        
        self._condition.acquire()
        try:
            if (self._maxconnections and self._connections >= self._maxconnections):
                raise TooManyConnections("%d connections are already equal to the max: %d" % (self._connections, self._maxconnections))
            # connection limit not reached, get a dedicated connection
            try: # first try to get it from the idle cache
                while True:
                    con = self._idle_cache.pop(0)
                    if not con.closed():
                        break
                print 'con cache'
            except IndexError: # else get a fresh connection
                con = self.make_connection()
            self._connections += 1
        finally:
            self._condition.release()
            print 'con get', self._connections

        return con

    def release(self, con):
        """Put a dedicated connection back into the idle cache."""
        self._condition.acquire()
        if con in self._idle_cache:
            # called via socket close on a connection in the idle cache
            self._condition.release()
            return
        try:
            if not self._maxcached or len(self._idle_cache) < self._maxcached:
                # the idle cache is not full, so put it there
                self._idle_cache.append(con)
            else: # if the idle cache is already full,
                logging.debug('dropping connection. connection pool (%s) is full. maxcached %s' % (len(self._idle_cache), self._maxcached))
                con.disconect() # then close the connection
            self._condition.notify()
        finally:
            self._connections -= 1
            self._condition.release()
            print 'con release', self._connections
    
    def close(self):
        """Close all connections in the pool."""
        self._condition.acquire()
        try:
            while self._idle_cache: # close all idle connections
                con = self._idle_cache.pop(0)
                try:
                    con.disconect()
                except Exception:
                    pass
                self._connections -=1
            self._condition.notifyAll()
        finally:
            self._condition.release()
    

