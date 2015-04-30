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

import json
import unittest

from obelix_client.queue import RedisQueue
from obelix_client.storage import RedisMock


class TestQueue(unittest.TestCase):

    def setUp(self):
        """ setup any state tied to the execution of the given method in a
        class.  setup_method is invoked for every test method of a class.
        """
        pass

    def test_redis_mock_queue_lpush_rpop(self):
        queue = RedisMock()

        data1 = {"test": [1, 2, 3]}
        data2 = [1, 2, 3]
        queue.lpush("q1", 111111)
        queue.lpush("q2", 222222)
        queue.lpush("q1", data1)
        queue.lpush("q2", data2)
        queue.lpush("q1", 555555)
        queue.lpush("q2", 666666)

        assert queue.rpop("q1") == 111111
        assert queue.rpop("q1") == data1
        assert queue.rpop("q1") == 555555

        assert queue.rpop("q2") == 222222
        assert queue.rpop("q2") == data2
        assert queue.rpop("q2") == 666666

    def test_redis_queue_lpush_rpop_with_encoder_prefix(self):
        queue = RedisQueue(RedisMock(), prefix='pre::', encoder=json)

        data1 = {"test": [1, 2, 3]}
        data2 = [1, 2, 3]
        queue.lpush("q1", 111111)
        queue.lpush("q2", 222222)
        queue.lpush("q1", data1)
        queue.lpush("q2", data2)
        queue.lpush("q1", 555555)
        queue.lpush("q2", 666666)

        assert queue.rpop("q1") == 111111
        assert queue.rpop("q1") == data1
        assert queue.rpop("q1") == 555555

        assert queue.rpop("q2") == 222222
        assert queue.rpop("q2") == data2
        assert queue.rpop("q2") == 666666

    def test_redis_queue_rpush_lpop_with_encoder_prefix(self):
        queue = RedisQueue(RedisMock(), prefix='pre::', encoder=json)

        data1 = {"test": [1, 2, 3]}
        data2 = [1, 2, 3]
        queue.rpush("q1", 111111)
        queue.rpush("q2", 222222)
        queue.rpush("q1", data1)
        queue.rpush("q2", data2)
        queue.rpush("q1", 555555)
        queue.rpush("q2", 666666)

        assert queue.lpop("q1") == 111111
        assert queue.lpop("q1") == data1
        assert queue.lpop("q1") == 555555

        assert queue.lpop("q2") == 222222
        assert queue.lpop("q2") == data2
        assert queue.lpop("q2") == 666666

    def test_lpush_and_rpop(self):
        queue = RedisQueue(RedisMock())
        #  Fill queue One and Two
        for i in range(0, 12):
            queue.lpush("One", i)
            queue.lpush("Two", i+30)

        # Check
        for i in range(0, 12):
            assert queue.rpop("One") == i
            assert queue.rpop("Two") == i+30
