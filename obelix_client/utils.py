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

"""Obelix-Client utils."""


def rank_records_by_order(conf, hitset):
    """
    Rank the records by the original order they we're provided.

    If the SearchEngineObelix([1,2,3]) then this method will
    return [1.0, 0.933, 0.867]

    Typically called like this:
        records, scores = __build_ranked_by_order()

    :return:
        A tuple, one list with records and one with scores.
        The list of records are integers while the scores
        are floats: [1,2,3],[.9,.8,7] etc...
    """
    if len(hitset) == 1:
        return {hitset[0]: conf['score_one_result']}
    elif not hitset:
        return {}

    upper = 1
    lower = conf['score_lower_limit']
    size = len(hitset)

    if size < conf['score_min_limit']:
        size *= conf['score_min_multiply']

    step = ((upper - lower) * 1.0 / size)
    scores = [1 - (lower + i * step) for i in range(0, size)]
    scores = scores[0:len(hitset)]

    ranked_hitset = {}
    for idx, score in enumerate(scores):
        ranked_hitset[hitset[idx]] = score

    return ranked_hitset


def sort_records_by_score(rec_scores):
    """
    Sort the records by score.

    :param record_scores: dictionary {1:0.2, 2:0.3 etc...
    :return: a tuple with two lists, the first is a list of records
    and the second of scores
    """
    records = []
    scores = []

    for recid in sorted(rec_scores, key=rec_scores.get, reverse=True):
        records.append(recid)
        scores.append(rec_scores[recid])

    return records, scores


def calc_scores(config, records_by_order, recommendations):
    """
    Calculate the scores based on the records and the recommendations.

    :param records_by_order: dictionary {1:0,2. recid: score,}
    :param recommendations: dictionary
    """
    final_scores = {}

    for recid, rec_score in records_by_order.items():
        recommendation_score = recommendations.get(recid, 0)
        score = (rec_score * (1 - config['recommendations_impact']) +
                 recommendation_score * config['recommendations_impact'])

        final_scores[recid] = score

    return final_scores


class SendToObelix(object):

    """Save data to the Obelix queue."""

    def __init__(self, queue):
        self.queue = queue

    def statistics_search_result(self, data):
        """Push to statistics_search_result."""
        self.queue.lpush("statistics-search-result", data)

    def statistics_page_view(self, data):
        """Push to statistics_page_view."""
        self.queue.lpush("statistics-page-view", data)

    def save_to_neo_feeder(self, data):
        """Push to logentries."""
        self.queue.lpush("logentries", data)
