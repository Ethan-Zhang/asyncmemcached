asyncmemcached
==============

An asynchronous library for accessing memcached with tornado.ioloop

feature
-----
* simple easy use
* connection pool


Usage
-----
    import asyncmemcached

    class Handler(tornado.web.RequestHandler):
        @property
        def db(self):
            if not hasattr(self, '_db'):
                self._db = asyncmemcached.Client(host='127.0.0.1', port=27017, maxcached=10, maxconnections=50)
            return self._db
    
        @tornado.web.asynchronous
        def get(self):
            self.db.get('keys', callback=self._on_response)
            # or
            # self.db.set('keys', 'value', cached_time, callback=self._on_response)
    
        def _on_response(self, response):
            self.render('template', full_name=response['full_name'])
  
  
Issues
------

Please report any issues via [github issues](https://github.com/Ethan-Zhang/asyncmemcached/issues)
