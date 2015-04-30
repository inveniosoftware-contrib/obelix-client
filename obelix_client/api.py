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

"""Obelix-Client Search Engine."""

import logging
import re
import time

from . import utils


CONFIG = {
    'recommendations_impact': 0.5,
    'score_lower_limit': 0.2,
    'score_upper_limit': 10,
    'score_min_limit': 10,
    'score_min_multiply': 4,
    'score_one_result': 1,
    'method_switch_limit': 20,
    'user_identifier': 'uid',
}


def get_logger():
    """Get a Logger."""
    return logging.getLogger('obelix_client')


class Obelix(object):

    """Obelix-Client."""

    def __init__(self, cache_storage, recommendation_storage, queue_storage,
                 config=None,
                 logger=None):
        """Initialize the Obelix-Client connector."""
        self.logger = logger or get_logger()
        self.recommendations = recommendation_storage
        self.cache = cache_storage
        self.send_to_obelix = utils.SendToObelix(queue_storage)
        self.config = CONFIG.copy()
        if config is not None:
            self.config.update(config)

        self.cache.set("settings", self.config)

    def rank_records(self, hitset, user_id, rg=10, jrec=0):
        """
        Rank a given search result based on recommendations.

        Expects the hitset to be sorted by latest last [1,2,3,4,5] (recids)
        :return:
            A tuple, one list with records and one with scores.
            The list of records are integers while the scores are floats:
                [1,2,3],[.9,.8,7] etc...
        """
        hitset = list(hitset)
        hitset.reverse()
        jrec = max(jrec - 1, 0)
        # TODO: Maybe cache ranked result, if the next page is loaded
        records_by_order = utils.rank_records_by_order(self.config, hitset)

        # Get Recommendations from storage
        recommendations = self.recommendations.get(user_id)

        # If the user does not have any recommendations, we can just return
        if (recommendations is None or
                self.config['recommendations_impact'] == 0):
            final_scores = records_by_order
        else:
            # Calculate scores
            final_scores = utils.calc_scores(self.config,
                                             records_by_order,
                                             recommendations)

        records, scores = utils.sort_records_by_score(final_scores)

        return records[jrec:jrec + rg], scores[jrec:jrec + rg]

    def log(self, action, *args, **kwargs):
        """Forward the log event."""
        return getattr(self, 'log_' + action)(*args, **kwargs)

    def log_search_result(self, user_info, original_result_ordered,
                          record_ids, results_final_colls_scores,
                          cols_in_result_ordered,
                          seconds_to_rank_and_print, jrec, rg, rm, cc):
        """
        Log a search result, used for statistics and to lookup last search.

        :param user_info:
        :param original_result_ordered:
        :param record_ids:
        :param results_final_colls_scores:
        :param cols_in_result_ordered:
        :param seconds_to_rank_and_print:
        :param jrec:
        :param rg:
        :param rm:
        :param cc:
        :return:
        """
        uid = user_info.get(self.config['user_identifier'])
        search_timestamp = time.time()

        # Store the current search to use with page views later
        data = {'search_timestamp': search_timestamp,
                'record_ids': record_ids,
                'jrec': jrec,
                'rm': rm,
                'rg': rg,
                'cc': cc}
        storage_key = "{0}::{1}".format("last-search-result", uid)
        self.cache.set(storage_key, data)

        # Store search result for statistics
        data = {'obelix_redis': "CFG_WEBSEARCH_OBELIX_REDIS",
                'obelix_uid': self.config['user_identifier'],
                'result': record_ids,
                'original_result_ordered': original_result_ordered,
                'results_final_colls_scores': results_final_colls_scores,
                'uid': uid,
                'remote_ip': user_info.get("remote_ip"),
                'uri': user_info.get('uri'),
                'timestamp': search_timestamp,
                'settings': self.config,
                'recommendations': self.recommendations.get(uid),
                'seconds_to_rank_and_print': seconds_to_rank_and_print,
                'cols_in_result_ordered': cols_in_result_ordered,
                'jrec': jrec,
                'rg': rg,
                'rm': rm,
                'cc': cc}
        self.send_to_obelix.statistics_search_result(data)

    def log_page_view_after_search(self, user_info, recid):
        """
        Log a page view.

        :param user_info:
        :param recid:
        :return:
        # TODO: Test if the page view happened from a search
        """
        self.log_page_view(user_info, recid,
                           req_type="events.pageviews",
                           file_format="page_view")

    def log_download_after_search(self, user_info, recid):
        """
        Log a download.

        We want to store downloads of PDFs as views
        (because users may click directly on download)
        :param user_info:
        :param recid:
        :return:
        """
        # if 'uri' in user_info and '.pdf' in user_info['uri'].lower():
        if 'uri' in user_info and 'subformat=' not in user_info['uri'].lower():
            p = re.compile(r'\.\D+')
            file_type = p.search(user_info['uri']).group()[1:]
            self.log_page_view(user_info, recid,
                               req_type="events.downloads",
                               file_format=file_type)

    def log_page_view(self, user_info, recid, req_type="events.pageviews",
                      file_format="view"):
        """Log a page view."""
        uid = user_info.get('uid')
        ip = user_info.get('remote_ip')
        uri = user_info.get('uri')

        self.log_page_view_for_neo_feeder(uid, recid, ip,
                                          req_type, file_format)
        self.log_page_view_for_analytics(uid, recid, ip, uri, req_type,
                                         user_info=user_info)

    def log_page_view_for_neo_feeder(self, uid, recid, remote_ip,
                                     req_type, file_format):
        """
        Feed the Obelix NeoFeeder with page views, used to construct the graph.

        :param uid:
        :param recid:
        :return: None
        """
        data = {
            'item': recid,
            'ip': remote_ip,
            "type": req_type,
            'user': uid,
            'file_format': file_format,
            "timestamp": time.time()
        }
        # goes to "logentries"
        self.send_to_obelix.save_to_neo_feeder(data)

    def log_page_view_for_analytics(self, uid, recid, ip, uri, req_type,
                                    user_info=None):
        """Mainly used to store statistics, may be removed in the future.

        :param uid:
        :param recid:
        :param ip:
        :param uri:
        :return:
        """
        storage_key = "{0}::{1}".format("last-search-result", uid)
        last_search_info = self.cache.get(storage_key)

        if not last_search_info:
            return

        hit_number_global = 0
        for collection_result in last_search_info['record_ids']:
            if recid in collection_result:
                hit_number_local = collection_result.index(recid)
                hit_number_global += hit_number_local

                timestamp = last_search_info['search_timestamp']
                jrec = last_search_info['jrec']
                rg = last_search_info['rg']
                rm = last_search_info['rm']
                cc = last_search_info['cc']

                recommendations = self.recommendations.get(uid, {})
                data = {'search_timestamp': timestamp,
                        'recid': recid,
                        'timestamp': time.time(),
                        'uid': uid,
                        'remote_ip': ip,
                        'uri': uri,
                        'jrec': jrec,
                        'rg': rg,
                        'rm': rm,
                        'cc': cc,
                        'hit_number_local': jrec + hit_number_local,
                        'hit_number_global': jrec + hit_number_global,
                        'recommendations': recommendations,
                        'recid_in_recommendations': recid in recommendations,
                        'type': req_type,
                        'user_info': user_info}
                self.send_to_obelix.statistics_page_view(data)

            hit_number_global += len(collection_result)
