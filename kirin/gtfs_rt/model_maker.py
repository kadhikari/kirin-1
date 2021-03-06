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
import datetime
from kirin import gtfs_realtime_pb2
import logging

from kirin import core
from kirin.core import model
from kirin.exceptions import KirinException, InvalidArguments, ObjectNotFound
from kirin.utils import make_navitia_wrapper, make_rt_update



def handle(proto, navitia_wrapper, contributor):
    data = str(proto)  # temp, for the moment, we save the protobuf as text
    rt_update = make_rt_update(data, 'gtfs-rt')
    try:
        trip_updates = KirinModelBuilder(navitia_wrapper, contributor).build(rt_update, data=proto)
    except KirinException as e:
        rt_update.status = 'KO'
        rt_update.error = e.data['error']
        model.db.session.add(rt_update)
        model.db.session.commit()
        raise
    except Exception as e:
        rt_update.status = 'KO'
        rt_update.error = e.message
        model.db.session.add(rt_update)
        model.db.session.commit()
        raise

    core.handle(rt_update, trip_updates, contributor)


def to_str(date):
    # the date is in UTC, thus we don't have to care about the coverage's timezone
    # TODO I don't understand why it doesn't work with UTC, so for now it's in  local
    return date.strftime("%Y%m%dT%H%M%S")


class KirinModelBuilder(object):

    def __init__(self, nav, contributor=None):
        self.navitia = nav
        self.contributor = contributor
        self.log = logging.getLogger(__name__)
        # TODO better period handling
        self.period_filter_tolerance = datetime.timedelta(hours=3)
        self.stop_code_key = 'source'  # TODO conf

    def build(self, rt_update, data):
        """
        parse the protobuf in the rt_update object
        and return a list of trip updates

        The TripUpdates are not yet associated with the RealTimeUpdate
        """
        self.log.info("proto = {}".format(data))
        data_time = datetime.datetime.utcfromtimestamp(data.header.timestamp)

        trip_updates = []
        for entity in data.entity:
            if not entity.trip_update:
                continue
            tu = self._make_trip_updates(entity.trip_update, data_time=data_time)
            trip_updates.extend(tu)

        return trip_updates

    def _make_trip_updates(self, input_trip_update, data_time):
        vjs = self._get_navitia_vjs(input_trip_update.trip, data_time=data_time)

        trip_updates = []
        for vj in vjs:
            trip_update = model.TripUpdate(vj=vj)
            trip_update.contributor = self.contributor
            trip_updates.append(trip_update)

            for input_st_update in input_trip_update.stop_time_update:
                st_update = self._make_stoptime_update(input_st_update, vj.navitia_vj)
                if not st_update:
                    continue
                trip_update.stop_time_updates.append(st_update)

        return trip_updates

    def _get_navitia_vjs(self, trip, data_time):
        vj_source_code = trip.trip_id

        since = data_time - self.period_filter_tolerance
        until = data_time + self.period_filter_tolerance
        self.log.debug('searching for vj {} on [{}, {}[ in navitia'.format(vj_source_code, since, until))

        navitia_vjs = self.navitia.vehicle_journeys(q={
            'filter': 'vehicle_journey.has_code({}, {})'.format(self.stop_code_key, vj_source_code),
            'since': to_str(since),
            'until': to_str(until),
            'depth': '2',  # we need this depth to get the stoptime's stop_area
        })

        if not navitia_vjs:
            logging.getLogger(__name__).info('impossible to find vj {t} on [{s}, {u}['
                                             .format(t=vj_source_code,
                                                     s=since,
                                                     u=until))

        return [model.VehicleJourney(nav_vj, since.date()) for nav_vj in navitia_vjs]

    def _make_stoptime_update(self, input_st_update, navitia_vj):
        nav_st = self._get_navitia_stop_time(input_st_update, navitia_vj)

        if nav_st is None:
            self.log.debug('impossible to find stop point {} in the vj {}, skipping it'.format(
                input_st_update.stop_id, navitia_vj.get('id')))
            return None

        nav_stop = nav_st.get('stop_point', {})

        # TODO handle delay uncertainty
        # TODO handle schedule_relationship
        def read_delay(st_event):
            if st_event and st_event.delay:
                return datetime.timedelta(seconds=st_event.delay)
        dep_delay = read_delay(input_st_update.departure)
        arr_delay = read_delay(input_st_update.arrival)
        dep_status = 'none' if dep_delay is None else 'update'
        arr_status = 'none' if arr_delay is None else 'update'


        st_update = model.StopTimeUpdate(nav_stop, departure_delay=dep_delay, arrival_delay=arr_delay,
                                         dep_status=dep_status, arr_status=arr_status)

        return st_update

    def _get_navitia_stop_time(self, input_st_update, navitia_vj):
        # TODO use input_st_update.stop_sequence to get the right stop_time even for loops
        for s in navitia_vj.get('stop_times', []):
            if any(c['type'] == self.stop_code_key and c['value'] == input_st_update.stop_id
                   for c in s.get('stop_point', {}).get('codes', [])):
                return s
        return None
