# coding=utf-8

# Copyright (c) 2001-2017, Canal TP and/or its affiliates. All rights reserved.
#
# This file is part of Navitia,
#     the software to build cool stuff with public transport.
#
# Hope you'll enjoy and contribute to this project,
#     powered by Canal TP (www.canaltp.fr).
# Help us simplify mobility and open public transport:
#     a non ending quest to the responsive locomotion way of traveling!
#
# LICENCE: This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Stay tuned using
# twitter @navitia
# IRC #navitia on freenode
# https://groups.google.com/d/forum/navitia
# www.navitia.io

from __future__ import absolute_import, print_function, unicode_literals, division
import flask
from flask.globals import current_app
from flask_restful import Resource
from google.protobuf.message import DecodeError
from kirin.exceptions import InvalidArguments
import navitia_wrapper
from kirin.gtfs_rt import model_maker
from kirin import redis
from kirin.utils import manage_db_error


def _get_gtfs_rt(req):
    if not req.data:
        raise InvalidArguments("no gtfs_rt data provided")
    return req.data


class GtfsRT(Resource):
    def __init__(self):
        # TODO:
        #  For the future multi-gtfs_rt with configurations in the table contributor
        #  parameters values should be updated by parameters of config file if present.
        url = current_app.config[str("NAVITIA_URL")]
        token = current_app.config.get(str("NAVITIA_GTFS_RT_TOKEN"))
        timeout = current_app.config.get(str("NAVITIA_TIMEOUT"), 5)
        instance = current_app.config[str("NAVITIA_GTFS_RT_INSTANCE")]
        query_timeout = current_app.config.get(str("NAVITIA_QUERY_CACHE_TIMEOUT"), 600)
        pubdate_timeout = current_app.config.get(str("NAVITIA_PUBDATE_CACHE_TIMEOUT"), 600)
        self.navitia_wrapper = navitia_wrapper.Navitia(
            url=url,
            token=token,
            timeout=timeout,
            cache=redis,
            query_timeout=query_timeout,
            pubdate_timeout=pubdate_timeout,
        ).instance(instance)
        self.contributor = current_app.config[str("GTFS_RT_CONTRIBUTOR")]

    def post(self):
        raw_proto = _get_gtfs_rt(flask.globals.request)

        from kirin import gtfs_realtime_pb2

        # create a raw gtfs-rt obj, save the raw protobuf into the db
        proto = gtfs_realtime_pb2.FeedMessage()
        try:
            proto.ParseFromString(raw_proto)
        except DecodeError:
            # We save the non-decodable flux gtfs-rt
            manage_db_error(proto, "gtfs-rt", contributor=self.contributor, status="KO", error="Decode Error")
            raise InvalidArguments("invalid protobuf")
        else:
            model_maker.handle(proto, self.navitia_wrapper, self.contributor)
            return "OK", 200
