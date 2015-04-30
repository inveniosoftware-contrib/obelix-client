# -*- coding: utf-8 -*-
#
# This file is part of Obelix.
# Copyright (C) 2015 CERN.
#
# Obelix is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Obelix is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Obelix; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.


"""Obelix-Client Storage Proxy."""


class StorageProxy(object):

    """
    Generic Storage.

    Your storage only has to support get, and set with default values
    """

    def __init__(self, storage, prefix=None, encoder=None):
        """
        Initialize Storage.

        :encoder: has to support load() and dump()
        """
        self.prefix = prefix
        self.encoder = encoder
        self.storage = storage

    def get(self, key, default=None):
        """Get a key."""
        if self.prefix:
            key = "{0}{1}".format(self.prefix, key)

        try:
            data = self.storage.get(key)
            # Redis returns None not a exception
            if data is None:
                data = default
        except KeyError:
            data = default

            # encode only if default is not set
        if self.encoder and data and data is not default:
            data = self.encoder.loads(data)

        return data

    def set(self, key, value):
        """Set a key, value pair."""
        if self.prefix:
            key = "{0}{1}".format(self.prefix, key)

        if self.encoder:
            value = self.encoder.dumps(value)

        if hasattr(self.storage, 'set'):
            self.storage.set(key, value)
        else:
            self.storage[key] = value


class RedisStorage(StorageProxy):

    """
    Wrapper for Redis.

    Takes care of the Timeout
    """

    def __init__(self, storage, prefix=None, encoder=None):
        """Init RedisStorage."""
        super(RedisStorage, self).__init__(storage, prefix, encoder)

    def get(self, key, default=None):
        """Get a key."""
        return super(RedisStorage, self).get(key, default)

    def set(self, key, value):
        """Set a key, value pair."""
        super(RedisStorage, self).set(key, value)


class RedisMock(object):

    """
    Redis Mock.

    Implements Redis based on dictionary's
    """

    def __init__(self):
        """Initialize storage dicts."""
        self.storage = {}
        self.queues = {}

    def get(self, key, default=None):
        """Get a key."""
        return self.storage.get(key, default)

    def set(self, key, value):
        """Set a key, value pair."""
        self.storage[key] = value

    def lpush(self, queue, value):
        """Left Push to queue."""
        if not self.queues.get(queue):
            # Create queue
            self.queues[queue] = []
        self.queues[queue].insert(0, value)

    def rpush(self, queue, value):
        """Right Push to queue."""
        if not self.queues.get(queue):
            # Create queue
            self.queues[queue] = []
        self.queues[queue].append(value)

    def rpop(self, queue):
        """Right Pop from queue (Item gets removed)."""
        try:
            data = self.queues[queue].pop()
        except KeyError:
            data = None

        return data

    def lpop(self, queue):
        """Left Pop from queue (Item gets removed)."""
        try:
            data = self.queues[queue].pop(0)
        except KeyError:
            data = None

        return data


# class RESTStorage(object):
#
#     def __init__(self, base_url=None, ):
#         pass
#         # set_url or get_ulr
#         # base is not None
#
#     def get(self, key):
#         return requests.get(self.url.format(key=key), ).json
