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

from obelix_client.api import Obelix
from obelix_client.queue import RedisQueue
from obelix_client.storage import RedisMock, RedisStorage


class TestObelix(unittest.TestCase):

    def setUp(self):
        """ setup any state tied to the execution of the given method in a
        class.  setup_method is invoked for every test method of a class.
        """
        self.cache = RedisStorage(RedisMock(), prefix='pre::', encoder=json)
        self.recommendations = RedisStorage(RedisMock(), 'recommendations::')
        self.queues = RedisQueue(RedisMock(), encoder=json)
        self.obelix = Obelix(self.cache, self.recommendations, self.queues)

    def test_obelix_settings_init(self):
        obelix = Obelix(self.cache, self.recommendations, self.queues)
        conf = obelix.config
        assert conf['recommendations_impact'] == 0.5
        assert conf['score_lower_limit'] == 0.2
        assert conf['score_upper_limit'] == 10
        assert conf['score_min_limit'] == 10
        assert conf['score_min_multiply'] == 4
        assert conf['score_one_result'] == 1
        assert conf['method_switch_limit'] == 20

    def test_obelix_settings_parameter(self):
        # Set settings
        settings = {'recommendations_impact': 5.5,
                    'score_lower_limit': 2.2,
                    'score_upper_limit': 1.5,
                    'score_min_limit': 9,
                    'score_min_multiply': 7,
                    'score_one_result': 3,
                    'method_switch_limit': 13}
        # storage.set('settings', settings)
        obelix = Obelix(self.cache, self.recommendations,
                        self.queues, settings)
        conf = obelix.config

        assert isinstance(conf['recommendations_impact'], float)

        for key, value in settings.items():
            assert conf[key] == value

    def test_rank_records_no_recommendations(self):
        obelix = self.obelix
        uid = 1
        hitset = range(1, 30)

        # Set recommendations
        # None
        # Page one
        result1 = obelix.rank_records(hitset, uid, 10, 0)
        # Page two
        result2 = obelix.rank_records(hitset, uid, 10, 11)
        # Page 3
        result3 = obelix.rank_records(hitset, uid, 10, 21)

        # TODO Check result
        pre1 = ([29, 28, 27, 26, 25, 24, 23, 22, 21, 20],
                [0.8, 0.7724137931034483, 0.7448275862068965,
                    0.7172413793103448, 0.6896551724137931, 0.6620689655172414,
                    0.6344827586206896, 0.6068965517241379, 0.5793103448275861,
                    0.5517241379310345])
        pre2 = ([19, 18, 17, 16, 15, 14, 13, 12, 11, 10],
                [0.5241379310344827, 0.4965517241379309, 0.46896551724137925,
                    0.4413793103448276, 0.4137931034482758, 0.386206896551724,
                    0.35862068965517235, 0.3310344827586207,
                    0.3034482758620689, 0.2758620689655171])
        pre3 = ([9, 8, 7, 6, 5, 4, 3, 2, 1],
                [0.24827586206896535, 0.2206896551724138, 0.193103448275862,
                    0.16551724137931023, 0.13793103448275845,
                    0.1103448275862069, 0.08275862068965512,
                    0.055172413793103336, 0.027586206896551557])

        assert pre1 == result1
        assert pre2 == result2
        assert pre3 == result3

    def test_rank_records_with_two_recommendations(self):
        obelix = self.obelix
        uid = 1
        hitset = range(1, 30)

        # Set recommendations
        pre_reco = {5: 0.5, 20: 1.0}
        self.recommendations.set(uid, pre_reco)

        # Page one
        result1 = obelix.rank_records(hitset, uid, 10, 0)
        # Page two
        result2 = obelix.rank_records(hitset, uid, 10, 11)
        # Page 3
        result3 = obelix.rank_records(hitset, uid, 10, 21)

        # TODO Check results by hand
        pre1 = ([20, 29, 28, 27, 26, 25, 24, 5, 23, 22],
                [0.7758620689655172, 0.4, 0.38620689655172413,
                    0.37241379310344824, 0.3586206896551724,
                    0.3448275862068966, 0.3310344827586207, 0.3189655172413792,
                    0.3172413793103448, 0.30344827586206896])
        pre2 = ([21, 19, 18, 17, 16, 15, 14, 13, 12, 11],
                [0.28965517241379307, 0.26206896551724135, 0.24827586206896546,
                    0.23448275862068962, 0.2206896551724138,
                    0.2068965517241379, 0.193103448275862, 0.17931034482758618,
                    0.16551724137931034, 0.15172413793103445])
        pre3 = ([10, 9, 8, 7, 6, 4, 3, 2, 1],
                [0.13793103448275856, 0.12413793103448267, 0.1103448275862069,
                    0.096551724137931, 0.08275862068965512,
                    0.05517241379310345, 0.04137931034482756,
                    0.027586206896551668, 0.013793103448275779])

        assert pre1 == result1
        assert pre2 == result2
        assert pre3 == result3

    def test_rank_records_with_one_record(self):
        obelix = self.obelix
        uid = 1
        hitset = [8]

        # Set recommendations
        pre_reco = {5: 0.5, 20: 1.0}
        self.recommendations.set(uid, pre_reco)
        obelix.config['score_one_result'] = 1

        # Page one
        result1 = obelix.rank_records(hitset, uid, 10, 0)
        # Page two
        result2 = obelix.rank_records(hitset, uid, 10, 11)
        # Page 3
        result3 = obelix.rank_records(hitset, uid, 10, 21)

        # TODO Check results by hand
        pre1 = ([8], [0.5])
        pre2 = ([], [])
        pre3 = ([], [])

        assert pre1 == result1
        assert pre2 == result2
        assert pre3 == result3

    def test_rank_records_with_no_record(self):
        obelix = self.obelix
        uid = 1
        hitset = []

        # Set recommendations
        pre_reco = {5: 0.5, 20: 1.0}
        self.recommendations.set(uid, pre_reco)
        obelix.config['score_one_result'] = 1

        # Page one
        result1 = obelix.rank_records(hitset, uid, 10, 0)
        # Page two
        result2 = obelix.rank_records(hitset, uid, 10, 11)
        # Page 3
        result3 = obelix.rank_records(hitset, uid, 10, 21)

        # TODO Check results by hand
        pre1 = ([], [])
        pre2 = ([], [])
        pre3 = ([], [])

        assert pre1 == result1
        assert pre2 == result2
        assert pre3 == result3


class TestObelixLogging(unittest.TestCase):

    def setUp(self):
        """ setup any state tied to the execution of the given method in a
        class.  setup_method is invoked for every test method of a class.
        """
        self.cache = RedisStorage(RedisMock(), prefix='pre::', encoder=json)
        self.recommendations = RedisStorage(RedisMock(), 'recommendations::')
        self.queues = RedisQueue(RedisMock(), encoder=json)
        self.obelix = Obelix(self.cache, self.recommendations, self.queues)

    def test_log_search_result(self):
        obelix = self.obelix

        # def test_log_search(self):
        user_info = {'uid': 1, 'remote_ip': "127.0.0.1", "uri": "testuri"}
        record_ids = [[1, 88], [1, 2]]
        results_final_colls_scores = [[0.3, 0.5], [0.5, 0.2]]
        cols_in_result_ordered = ["Thesis", "Another"]
        seconds_to_rank_and_print = 2

        jrec, rg, rm, cc = 0, 10, "recommendations", "obelix"

        obelix.log("search_result", user_info, record_ids, record_ids,
                   results_final_colls_scores,
                   cols_in_result_ordered,
                   seconds_to_rank_and_print,
                   jrec, rg, rm, cc)

        storage_key = "{0}::{1}".format("last-search-result", user_info['uid'])
        log_storage = self.cache.get(storage_key)
        log_queue = self.queues.rpop("statistics-search-result")

        assert record_ids == log_storage['record_ids']
        assert results_final_colls_scores == \
            log_queue['results_final_colls_scores']

    def test_log_search_analytics(self):
        obelix = self.obelix

        user_info = {'uid': 1, 'remote_ip': "127.0.0.1", "uri": "testuri"}
        record_ids = [[1, 88], [1, 2]]
        results_final_colls_scores = [[0.3, 0.5], [0.5, 0.2]]
        cols_in_result_ordered = ["Thesis", "Another"]
        seconds_to_rank_and_print = 2

        jrec, rg, rm, cc = 0, 10, "recommendations", "obelix"

        obelix.log('search_result', user_info, record_ids, record_ids,
                   results_final_colls_scores,
                   cols_in_result_ordered,
                   seconds_to_rank_and_print,
                   jrec, rg, rm, cc)

        # TODO: Check if own queue are needed
        # log_queue = queues.lpop("statistics-search-result::1")
        log_queue = self.queues.lpop("statistics-search-result")

        assert str(user_info['uid']) == str(log_queue['uid'])
        assert user_info['remote_ip'] == log_queue['remote_ip']

    def test_log_page_view(self):
        obelix = self.obelix

        user_info = {'uid': 1, 'remote_ip': "127.0.0.1", "uri": "testuri"}
        record_ids = [[1, 88], [1, 2]]
        results_final_colls_scores = [[0.3, 0.5], [0.5, 0.2]]
        cols_in_result_ordered = ["Thesis", "Another"]
        seconds_to_rank_and_print = 2

        jrec, rg, rm, cc = 0, 10, "recommendations", "obelix"

        obelix.log('search_result', user_info, record_ids, record_ids,
                   results_final_colls_scores,
                   cols_in_result_ordered,
                   seconds_to_rank_and_print,
                   jrec, rg, rm, cc)
        obelix.log('page_view', user_info, 1)

        logged = self.queues.lpop("statistics-page-view")
        assert str(logged['uid']) == '1'
        # TODO: check cache "last-search-result"
        logged = self.queues.lpop("logentries")
        assert logged['type'] == "events.pageviews"
        assert str(logged['user']) == '1'

    def test_log_page_view_after_search_error(self):
        """ There should be no statistics
            because there was no search before
        """
        # TODO: Improve test
        obelix = self.obelix

        user_info = {'uid': 1, 'remote_ip': "127.0.0.1", "uri": "testuri"}
        record_ids = [[1, 88], [1, 2]]

        obelix.log('page_view_after_search', user_info, record_ids)

        logged = self.queues.lpop("statistics-page-view")
        assert logged is None
        # TODO: check cache "last-search-result"
        logged = self.queues.lpop("logentries")
        assert logged['type'] == "events.pageviews"
        assert str(logged['user']) == '1'

    def test_log_download_after_search(self):
        obelix = self.obelix

        user_info = {'uid': 5, 'remote_ip': "127.0.0.1", "uri": "testuri.pdf"}
        record_ids = [[1, 88], [1, 2]]
        results_final_colls_scores = [[0.3, 0.5], [0.5, 0.2]]
        cols_in_result_ordered = ["Thesis", "Another"]
        seconds_to_rank_and_print = 2
        downloaded_record = 88

        jrec, rg, rm, cc = 0, 10, "recommendations", "obelix"

        # log search
        obelix.log('search_result', user_info, record_ids, record_ids,
                   results_final_colls_scores,
                   cols_in_result_ordered,
                   seconds_to_rank_and_print,
                   jrec, rg, rm, cc)
        obelix.log('download_after_search', user_info, downloaded_record)
        # 'last-search-result::5'
        logged = self.queues.lpop("statistics-search-result")
        assert str(logged['uid']) == '5'
        # TODO: check cache "last-search-result"
        logged = self.queues.lpop("logentries")
        assert logged['type'] == "events.downloads"
        assert str(logged['user']) == '5'
