from setuptools import setup

setup(
    name="asyncmemcached",
    version='0.0.5',
    description="Asynchronous library for accessing memcached built upon the tornado IOLoop.",
    author="Ethan Zhang",
    author_email="networm@163.com",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    packages=['asyncmemcached'],
    requires=['tornado'],
)
