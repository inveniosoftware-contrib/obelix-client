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


"""Obelix-Client Queue Proxy."""


class RedisQueue(object):

    """Redis Queue Proxy, takes care of de/encoding."""

    def __init__(self, storage, prefix=None, encoder=None):
        """Init RedisQueue."""
        self.prefix = prefix
        self.encoder = encoder
        self.storage = storage

    def lpush(self, queue, value):
        """Left Push to queue and encode value."""
        if self.prefix:
            queue = "{0}{1}".format(self.prefix, queue)
        if self.encoder:
            value = self.encoder.dumps(value)

        self.storage.lpush(queue, value)

    def rpush(self, queue, value):
        """Right Push to queue and encode value."""
        if self.prefix:
            queue = "{0}{1}".format(self.prefix, queue)
        if self.encoder:
            value = self.encoder.dumps(value)

        self.storage.rpush(queue, value)

    def rpop(self, queue):
        """Right Pop from queue and decode value."""
        if self.prefix:
            queue = "{0}{1}".format(self.prefix, queue)
        data = self.storage.rpop(queue)
        if self.encoder and data is not None:
            data = self.encoder.loads(data)

        return data

    def lpop(self, queue):
        """Left Pop from queue and decode value."""
        if self.prefix:
            queue = "{0}{1}".format(self.prefix, queue)
        data = self.storage.lpop(queue)
        if self.encoder and data is not None:
            data = self.encoder.loads(data)

        return data
