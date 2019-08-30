# coding=utf-8

# Copyright (c) 2001-2015, Canal TP and/or its affiliates. All rights reserved.
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

from kirin import gtfs_realtime_pb2, kirin_pb2
from kirin.core.types import stop_time_status_to_protobuf
import datetime


def date_to_str(date):
    if date:
        return date.strftime("%Y%m%d")
    return None


def to_posix_time(date_time):
    if date_time:
        return int((date_time - datetime.datetime(1970, 1, 1)).total_seconds())
    return 0


def convert_to_gtfsrt(trip_updates, incrementality=gtfs_realtime_pb2.FeedHeader.DIFFERENTIAL):
    feed = gtfs_realtime_pb2.FeedMessage()

    feed.header.incrementality = incrementality
    feed.header.gtfs_realtime_version = "1"
    feed.header.timestamp = to_posix_time(datetime.datetime.utcnow())

    for trip_update in trip_updates:
        fill_entity(feed.entity.add(), trip_update)

    return feed


def get_st_event(st_status):
    if st_status in ("delete", "deleted_for_detour"):
        return gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
    elif st_status in ("add", "added_for_detour"):
        return gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ADDED
    else:
        # 'update' or 'none' are modeled as 'SCHEDULED'
        return gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SCHEDULED


def get_trip_event(trip_status):
    trip_events = {
        "NO_SERVICE": gtfs_realtime_pb2.Alert.NO_SERVICE,
        "REDUCED_SERVICE": gtfs_realtime_pb2.Alert.REDUCED_SERVICE,
        "SIGNIFICANT_DELAYS": gtfs_realtime_pb2.Alert.SIGNIFICANT_DELAYS,
        "DETOUR": gtfs_realtime_pb2.Alert.DETOUR,
        "ADDITIONAL_SERVICE": gtfs_realtime_pb2.Alert.ADDITIONAL_SERVICE,
        "MODIFIED_SERVICE": gtfs_realtime_pb2.Alert.MODIFIED_SERVICE,
        "OTHER_EFFECT": gtfs_realtime_pb2.Alert.OTHER_EFFECT,
        "UNKNOWN_EFFECT": gtfs_realtime_pb2.Alert.UNKNOWN_EFFECT,
        "STOP_MOVED": gtfs_realtime_pb2.Alert.STOP_MOVED,
    }
    return trip_events.get(trip_status, None)


def fill_stop_times(pb_stop_time, stop_time):
    pb_stop_time.stop_id = stop_time.stop_id
    pb_stop_time.arrival.time = to_posix_time(stop_time.arrival)
    if stop_time.arrival_delay:
        pb_stop_time.arrival.delay = int(stop_time.arrival_delay.total_seconds())
    else:
        pb_stop_time.arrival.delay = 0
    pb_stop_time.departure.time = to_posix_time(stop_time.departure)
    if stop_time.departure_delay:
        pb_stop_time.departure.delay = int(stop_time.departure_delay.total_seconds())
    else:
        pb_stop_time.departure.delay = 0

    """
    TODO: kirin_pb2.stop_time_event_relationship needs to be removed once
    kirin_pb2.stop_time_event_status is deployed on production
    """
    pb_stop_time.departure.Extensions[kirin_pb2.stop_time_event_relationship] = get_st_event(
        stop_time.departure_status
    )
    pb_stop_time.arrival.Extensions[kirin_pb2.stop_time_event_relationship] = get_st_event(
        stop_time.arrival_status
    )

    pb_stop_time.departure.Extensions[kirin_pb2.stop_time_event_status] = stop_time_status_to_protobuf(
        stop_time.departure_status
    )
    pb_stop_time.arrival.Extensions[kirin_pb2.stop_time_event_status] = stop_time_status_to_protobuf(
        stop_time.arrival_status
    )

    if stop_time.message:
        pb_stop_time.Extensions[kirin_pb2.stoptime_message] = stop_time.message


def fill_message(pb_trip_update, message):
    pb_trip_update.Extensions[kirin_pb2.trip_message] = message


def fill_trip_update(pb_trip_update, trip_update):
    pb_trip = pb_trip_update.trip
    if trip_update.contributor_id:
        pb_trip.Extensions[kirin_pb2.contributor] = trip_update.contributor_id
    if trip_update.message:
        fill_message(pb_trip_update, trip_update.message)
    if trip_update.company_id:
        pb_trip.Extensions[kirin_pb2.company_id] = trip_update.company_id
    if trip_update.effect:
        pb_trip_update.Extensions[kirin_pb2.effect] = get_trip_event(trip_update.effect)
    if trip_update.physical_mode_id:
        pb_trip_update.vehicle.Extensions[kirin_pb2.physical_mode_id] = trip_update.physical_mode_id
    if trip_update.headsign:
        pb_trip_update.Extensions[kirin_pb2.headsign] = trip_update.headsign

    vj = trip_update.vj
    if vj:
        pb_trip.trip_id = vj.navitia_trip_id
        # WARNING: here trip.start_date is considered UTC, not local
        # (this date differs if vj starts during the period between midnight UTC and local midnight)
        pb_trip.start_date = date_to_str(vj.get_utc_circulation_date())
        # TODO fill the right schedule_relationship
        if trip_update.status == "delete":
            pb_trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.CANCELED
        else:
            pb_trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.SCHEDULED

        for stop_time_update in trip_update.stop_time_updates:
            fill_stop_times(pb_trip_update.stop_time_update.add(), stop_time_update)


def fill_entity(pb_entity, trip_update):
    pb_entity.id = trip_update.vj_id
    fill_trip_update(pb_entity.trip_update, trip_update)
