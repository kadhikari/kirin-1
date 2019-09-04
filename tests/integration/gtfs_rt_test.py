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
from copy import deepcopy
from datetime import timedelta
import datetime
import pytest
from kirin.core.model import RealTimeUpdate, db, TripUpdate, StopTimeUpdate, VehicleJourney
from kirin.core.populate_pb import to_posix_time, convert_to_gtfsrt
from kirin import gtfs_rt
from kirin.core.types import TripEffect
from tests import mock_navitia
from tests.check_utils import dumb_nav_wrapper, api_post
from kirin import gtfs_realtime_pb2, app
from kirin.utils import save_gtfs_rt_with_error, manage_db_error
import time
from sqlalchemy import desc


@pytest.fixture(scope="function", autouse=True)
def navitia(monkeypatch):
    """
    Mock all calls to navitia for this fixture and get_publication_date
    """
    monkeypatch.setattr("navitia_wrapper._NavitiaWrapper.query", mock_navitia.mock_navitia_query)
    monkeypatch.setattr(
        "navitia_wrapper._NavitiaWrapper.get_publication_date", mock_navitia.mock_publication_date
    )


@pytest.fixture(scope="function")
def mock_rabbitmq(monkeypatch):
    """
    Mock all calls to navitia for this fixture
    """
    from mock import MagicMock

    mock_amqp = MagicMock()
    monkeypatch.setattr("kombu.messaging.Producer.publish", mock_amqp)
    return mock_amqp


@pytest.fixture()
def basic_gtfs_rt_data():
    feed = gtfs_realtime_pb2.FeedMessage()

    feed.header.gtfs_realtime_version = "1.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = to_posix_time(datetime.datetime(year=2012, month=6, day=15, hour=15))

    entity = feed.entity.add()
    entity.id = "bob"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "Code-R-vj1"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 60
    stu.stop_sequence = 2
    stu.stop_id = "Code-StopR2"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.stop_sequence = 3
    stu.stop_id = "Code-StopR3"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 180
    stu.stop_sequence = 4
    stu.stop_id = "Code-StopR4"

    return feed


@pytest.fixture()
def basic_gtfs_rt_data_without_delays():
    feed = gtfs_realtime_pb2.FeedMessage()

    feed.header.gtfs_realtime_version = "1.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = to_posix_time(datetime.datetime(year=2012, month=6, day=15, hour=15))

    entity = feed.entity.add()
    entity.id = "bob"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "Code-R-vj1"

    stu = trip_update.stop_time_update.add()
    stu.stop_sequence = 4
    stu.stop_id = "Code-StopR4"

    return feed


def test_wrong_gtfs_rt_post():
    """
    wrong protobuf post on the api
    """
    res, status = api_post("/gtfs_rt", check=False, data="bob")

    assert status == 400
    assert "invalid protobuf" in res.get("error")


def test_gtfs_rt_post_no_data():
    """
    when no data is given, we got a 400 error
    """
    tester = app.test_client()
    resp = tester.post("/gtfs_rt")
    assert resp.status_code == 400

    with app.app_context():
        assert len(RealTimeUpdate.query.all()) == 0
        assert len(TripUpdate.query.all()) == 0
        assert len(StopTimeUpdate.query.all()) == 0


def test_gtfs_model_builder(basic_gtfs_rt_data, basic_gtfs_rt_data_without_delays):
    """
    test the model builder with a simple gtfs-rt

    we have realtime data on only 3 stops, so the model builder should have 4 stops with that absent in
    realtime data created with no delay(the VJ has 4 stops)
    Note: The trip_update stop list is a strict ending sublist of of stops list of navitia_vj
    """
    with app.app_context():
        data = ""
        rt_update = RealTimeUpdate(data, connector="gtfs-rt", contributor="realtime.sherbrooke")
        trip_updates = gtfs_rt.KirinModelBuilder(dumb_nav_wrapper(), contributor="realtime.sherbrooke").build(
            rt_update, basic_gtfs_rt_data
        )

        # we associate the trip_update manually for sqlalchemy to make the links
        rt_update.trip_updates = trip_updates
        db.session.add(rt_update)
        db.session.commit()

        assert rt_update.contributor_id == "realtime.sherbrooke"
        assert len(trip_updates) == 1
        assert len(trip_updates[0].stop_time_updates) == 4
        assert trip_updates[0].effect == "SIGNIFICANT_DELAYS"
        assert trip_updates[0].contributor_id == "realtime.sherbrooke"

        # stop_time_update created with no delay
        first_stop = trip_updates[0].stop_time_updates[0]
        assert first_stop.stop_id == "StopR1"
        assert first_stop.arrival_status == "none"
        assert first_stop.arrival_delay is None
        assert first_stop.departure_delay is None
        assert first_stop.departure_status == "none"
        assert first_stop.message is None

        second_stop = trip_updates[0].stop_time_updates[1]
        assert second_stop.stop_id == "StopR2"
        assert second_stop.arrival_status == "update"
        assert second_stop.arrival_delay == timedelta(minutes=1)
        assert second_stop.departure_delay is None
        assert second_stop.departure_status == "none"
        assert second_stop.message is None

        fourth_stop = trip_updates[0].stop_time_updates[3]
        assert fourth_stop.stop_id == "StopR4"
        assert fourth_stop.arrival_status == "update"
        assert fourth_stop.arrival_delay == timedelta(minutes=3)
        assert fourth_stop.departure_delay is None
        assert fourth_stop.departure_status == "none"
        assert fourth_stop.message is None

        feed = convert_to_gtfsrt(trip_updates)
        assert feed.entity[0].trip_update.trip.start_date == "20120615"  # must be UTC start date

        # if there is no delay field (delay is optional in StopTimeEvent), effect = 'UNKNOWN_EFFECT'
        rt_update = RealTimeUpdate(data, connector="gtfs-rt", contributor="realtime.sherbrooke")
        trip_updates = gtfs_rt.KirinModelBuilder(dumb_nav_wrapper(), contributor="realtime.sherbrooke").build(
            rt_update, basic_gtfs_rt_data_without_delays
        )
        assert len(trip_updates) == 1
        assert trip_updates[0].effect == "UNKNOWN_EFFECT"


def test_gtfs_rt_simple_delay(basic_gtfs_rt_data, mock_rabbitmq):
    """
    test the gtfs-rt post with a simple gtfs-rt

    we have realtime data on only 3 stops, so the model builder should have 4 stops with that absent in
    realtime data created with no delay(the VJ has 4 stops)

    after the merge, we should have 4 stops (and only 2 delayed)
    """
    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=basic_gtfs_rt_data.SerializeToString())
    assert resp.status_code == 200

    with app.app_context():
        assert len(RealTimeUpdate.query.all()) == 1
        assert len(TripUpdate.query.all()) == 1
        assert len(StopTimeUpdate.query.all()) == 4

        trip_update = TripUpdate.find_by_dated_vj("R:vj1", datetime.datetime(2012, 6, 15, 14, 0))

        assert trip_update

        # navitia's time are in local, but we return UTC time, and the stop is in sherbrooke, so UTC-4h
        # stop_time_update created with no delay
        first_stop = trip_update.stop_time_updates[0]
        assert first_stop.stop_id == "StopR1"
        assert first_stop.arrival_status == "none"
        assert first_stop.arrival_delay == timedelta(0)
        assert first_stop.arrival == datetime.datetime(2012, 6, 15, 14, 00)
        assert first_stop.departure_delay == timedelta(0)
        assert first_stop.departure_status == "none"
        assert first_stop.departure == datetime.datetime(2012, 6, 15, 14, 00)
        assert first_stop.message is None

        second_stop = trip_update.stop_time_updates[1]
        assert second_stop.stop_id == "StopR2"
        assert second_stop.arrival_status == "update"
        # 10:30 in local + 4h to get it in UTC + 1minute of delay
        assert second_stop.arrival == datetime.datetime(2012, 6, 15, 14, 31)
        assert second_stop.arrival_delay == timedelta(minutes=1)
        # even if the GTFS-RT has no information of the departure, it have been also delayed by 1mn
        # for coherence
        assert second_stop.departure == datetime.datetime(2012, 6, 15, 14, 31)
        assert second_stop.departure_delay == timedelta(minutes=1)
        assert second_stop.departure_status == "none"
        assert second_stop.message is None

        third_stop = trip_update.stop_time_updates[2]
        assert third_stop.stop_id == "StopR3"
        assert third_stop.arrival_status == "none"
        assert third_stop.arrival_delay == timedelta(0)
        assert third_stop.arrival == datetime.datetime(2012, 6, 15, 15, 00)
        assert third_stop.departure_delay == timedelta(0)
        assert third_stop.departure_status == "none"
        assert third_stop.departure == datetime.datetime(2012, 6, 15, 15, 00)
        assert third_stop.message is None

        fourth_stop = trip_update.stop_time_updates[3]
        assert fourth_stop.stop_id == "StopR4"
        assert fourth_stop.arrival_status == "update"
        assert fourth_stop.arrival_delay == timedelta(minutes=3)
        assert fourth_stop.arrival == datetime.datetime(2012, 6, 15, 15, 33)
        # even if the GTFS-RT has no information of the departure, it have been also delayed by 3mn
        # for coherence
        assert fourth_stop.departure_delay == timedelta(minutes=3)
        assert fourth_stop.departure_status == "none"
        assert fourth_stop.departure == datetime.datetime(2012, 6, 15, 15, 33)
        assert fourth_stop.message is None


def test_gtfs_rt_purge(basic_gtfs_rt_data, mock_rabbitmq):
    """
    POST a simple gtfs-rt, then test the purge
    """
    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=basic_gtfs_rt_data.SerializeToString())
    assert resp.status_code == 200

    with app.app_context():
        # Check there's really something before purge
        assert len(RealTimeUpdate.query.all()) == 1
        assert len(TripUpdate.query.all()) == 1
        assert len(VehicleJourney.query.all()) == 1
        assert len(StopTimeUpdate.query.all()) == 4
        assert db.session.execute("select * from associate_realtimeupdate_tripupdate").rowcount == 1

        # VehicleJourney affected is old, so it's affected by TripUpdate purge (based on base-VJ's date)
        contrib = app.config.get(str("GTFS_RT_CONTRIBUTOR"))
        until = datetime.date(2012, 12, 31)
        TripUpdate.remove_by_contributors_and_period(contributors=[contrib], start_date=None, end_date=until)

        assert len(TripUpdate.query.all()) == 0
        assert len(VehicleJourney.query.all()) == 0
        assert len(StopTimeUpdate.query.all()) == 0
        assert db.session.execute("select * from associate_realtimeupdate_tripupdate").rowcount == 0
        assert len(RealTimeUpdate.query.all()) == 1  # keeping RTU longer for potential debug need

        # Put an old (realistic) date to RealTimeUpdate object so that RTU purge affects it
        rtu = RealTimeUpdate.query.all()[0]
        rtu.created_at = datetime.datetime(2012, 6, 15, 15, 33)

        connector = "gtfs-rt"
        RealTimeUpdate.remove_by_connectors_until(connectors=[connector], until=until)

        assert len(TripUpdate.query.all()) == 0
        assert len(VehicleJourney.query.all()) == 0
        assert len(StopTimeUpdate.query.all()) == 0
        assert db.session.execute("select * from associate_realtimeupdate_tripupdate").rowcount == 0
        assert len(RealTimeUpdate.query.all()) == 0


@pytest.fixture()
def pass_midnight_gtfs_rt_data():
    feed = gtfs_realtime_pb2.FeedMessage()

    feed.header.gtfs_realtime_version = "1.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = to_posix_time(datetime.datetime(year=2012, month=6, day=16, hour=5))

    entity = feed.entity.add()
    entity.id = "bob"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "Code-pass-midnight"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 60
    stu.departure.delay = 60
    stu.stop_sequence = 1
    stu.stop_id = "Code-StopR1"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 60
    stu.departure.delay = 60
    stu.stop_sequence = 2
    stu.stop_id = "Code-StopR2"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 150
    stu.departure.delay = 150
    stu.stop_sequence = 3
    stu.stop_id = "Code-StopR2-bis"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 180
    stu.departure.delay = 180
    stu.stop_sequence = 4
    stu.stop_id = "Code-StopR3"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 240
    stu.departure.delay = 240
    stu.stop_sequence = 5
    stu.stop_id = "Code-StopR4"

    return feed


def test_gtfs_pass_midnight_model_builder(pass_midnight_gtfs_rt_data):
    """
    test the model builder with a pass-midnight gtfs-rt
    """
    with app.app_context():
        data = ""
        rt_update = RealTimeUpdate(data, connector="gtfs-rt", contributor="realtime.sherbrooke")
        trip_updates = gtfs_rt.KirinModelBuilder(dumb_nav_wrapper(), contributor="realtime.sherbrooke").build(
            rt_update, pass_midnight_gtfs_rt_data
        )

        # we associate the trip_update manually for sqlalchemy to make the links
        rt_update.trip_updates = trip_updates
        db.session.add(rt_update)
        db.session.commit()

        assert len(trip_updates) == 1
        assert len(trip_updates[0].stop_time_updates) == 5
        assert RealTimeUpdate.query.first().status == "OK"

        first_stop = trip_updates[0].stop_time_updates[0]
        assert first_stop.stop_id == "StopR1"
        assert first_stop.arrival_status == "update"
        assert first_stop.arrival_delay == timedelta(minutes=1)
        assert first_stop.departure_status == "update"
        assert first_stop.departure_delay == timedelta(minutes=1)
        assert first_stop.message is None

        second_stop = trip_updates[0].stop_time_updates[1]
        assert second_stop.stop_id == "StopR2"
        assert second_stop.arrival_status == "update"
        assert second_stop.arrival_delay == timedelta(minutes=1)
        assert second_stop.departure_status == "update"
        assert second_stop.departure_delay == timedelta(minutes=1)
        assert second_stop.message is None

        second_stop = trip_updates[0].stop_time_updates[2]
        assert second_stop.stop_id == "StopR2-bis"
        assert second_stop.arrival_status == "update"
        assert second_stop.arrival_delay == timedelta(minutes=2, seconds=30)
        assert second_stop.departure_status == "update"
        assert second_stop.departure_delay == timedelta(minutes=2, seconds=30)
        assert second_stop.message is None

        third_stop = trip_updates[0].stop_time_updates[3]
        assert third_stop.stop_id == "StopR3"
        assert third_stop.arrival_status == "update"
        assert third_stop.arrival_delay == timedelta(minutes=3)
        assert third_stop.departure_status == "update"
        assert third_stop.departure_delay == timedelta(minutes=3)
        assert third_stop.message is None

        fourth_stop = trip_updates[0].stop_time_updates[4]
        assert fourth_stop.stop_id == "StopR4"
        assert fourth_stop.arrival_status == "update"
        assert fourth_stop.arrival_delay == timedelta(minutes=4)
        assert fourth_stop.departure_status == "update"
        assert fourth_stop.departure_delay == timedelta(minutes=4)
        assert fourth_stop.message is None

        feed = convert_to_gtfsrt(trip_updates)
        assert feed.entity[0].trip_update.trip.start_date == "20120616"  # must be UTC start date


def test_gtfs_rt_pass_midnight(pass_midnight_gtfs_rt_data, mock_rabbitmq):
    """
    test the gtfs-rt post with a pass-midnight gtfs-rt

    we have realtime data on all 5 stops

    after the merge, we should have 5 stops properly delayed
    """
    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=pass_midnight_gtfs_rt_data.SerializeToString())
    assert resp.status_code == 200

    with app.app_context():
        assert len(RealTimeUpdate.query.all()) == 1
        assert len(TripUpdate.query.all()) == 1
        assert len(StopTimeUpdate.query.all()) == 5
        assert RealTimeUpdate.query.first().status == "OK"

        trip_update = TripUpdate.find_by_dated_vj("R:vj1", datetime.datetime(2012, 6, 16, 3, 30))
        assert trip_update.contributor_id == "realtime.sherbrooke"
        assert trip_update

        assert trip_update.vj.get_start_timestamp() == datetime.datetime(2012, 6, 16, 3, 30)

        # using navitia's time in UTC, and the stop is in sherbrooke, so UTC-4h
        first_stop = trip_update.stop_time_updates[0]
        assert first_stop.stop_id == "StopR1"
        assert first_stop.arrival_status == "update"
        assert first_stop.arrival_delay == timedelta(minutes=1)
        # 23:30 in local + 4h to get it in UTC + 1minute of delay
        assert first_stop.arrival == datetime.datetime(2012, 6, 16, 3, 31)
        assert first_stop.departure_delay == timedelta(minutes=1)
        assert first_stop.departure_status == "update"
        assert first_stop.departure == datetime.datetime(2012, 6, 16, 3, 31)
        assert first_stop.message is None

        second_stop = trip_update.stop_time_updates[1]
        assert second_stop.stop_id == "StopR2"
        assert second_stop.arrival_status == "update"
        assert second_stop.arrival == datetime.datetime(2012, 6, 16, 4, 0)
        assert second_stop.arrival_delay == timedelta(minutes=1)
        assert second_stop.departure == datetime.datetime(2012, 6, 16, 4, 1)
        assert second_stop.departure_delay == timedelta(minutes=1)
        assert second_stop.departure_status == "update"
        assert second_stop.message is None

        second_stop = trip_update.stop_time_updates[2]
        assert second_stop.stop_id == "StopR2-bis"
        assert second_stop.arrival_status == "update"
        assert second_stop.arrival == datetime.datetime(2012, 6, 16, 4, 2, 30)
        assert second_stop.arrival_delay == timedelta(minutes=2, seconds=30)
        assert second_stop.departure == datetime.datetime(2012, 6, 16, 4, 2, 30)
        assert second_stop.departure_delay == timedelta(minutes=2, seconds=30)
        assert second_stop.departure_status == "update"
        assert second_stop.message is None

        third_stop = trip_update.stop_time_updates[3]
        assert third_stop.stop_id == "StopR3"
        assert third_stop.arrival_status == "update"
        assert third_stop.arrival_delay == timedelta(minutes=3)
        assert third_stop.arrival == datetime.datetime(2012, 6, 16, 4, 3)
        assert third_stop.departure_delay == timedelta(minutes=3)
        assert third_stop.departure_status == "update"
        assert third_stop.departure == datetime.datetime(2012, 6, 16, 4, 4)
        assert third_stop.message is None

        fourth_stop = trip_update.stop_time_updates[4]
        assert fourth_stop.stop_id == "StopR4"
        assert fourth_stop.arrival_status == "update"
        assert fourth_stop.arrival_delay == timedelta(minutes=4)
        assert fourth_stop.arrival == datetime.datetime(2012, 6, 16, 4, 34)
        assert fourth_stop.departure_delay == timedelta(minutes=4)
        assert fourth_stop.departure_status == "update"
        assert fourth_stop.departure == datetime.datetime(2012, 6, 16, 4, 34)
        assert fourth_stop.message is None


@pytest.fixture()
def pass_midnight_utc_gtfs_rt_data():
    """
    Port tests for pass-midnight UTC also
    """
    feed = deepcopy(pass_midnight_gtfs_rt_data())
    feed.header.timestamp = to_posix_time(datetime.datetime(year=2012, month=6, day=16, hour=1))
    feed.entity[0].trip_update.trip.trip_id = "Code-pass-midnight-UTC"

    return feed


def test_gtfs_pass_midnight_utc_model_builder(pass_midnight_utc_gtfs_rt_data):
    """
    test the model builder with a pass-midnight UTC gtfs-rt
    """
    with app.app_context():
        data = ""
        rt_update = RealTimeUpdate(data, connector="gtfs-rt", contributor="realtime.sherbrooke")
        trip_updates = gtfs_rt.KirinModelBuilder(dumb_nav_wrapper(), contributor="realtime.sherbrooke").build(
            rt_update, pass_midnight_utc_gtfs_rt_data
        )

        # we associate the trip_update manually for sqlalchemy to make the links
        rt_update.trip_updates = trip_updates
        db.session.add(rt_update)
        db.session.commit()

        assert len(trip_updates) == 1
        assert len(trip_updates[0].stop_time_updates) == 5
        assert RealTimeUpdate.query.first().status == "OK"

        first_stop = trip_updates[0].stop_time_updates[0]
        assert first_stop.stop_id == "StopR1"
        assert first_stop.arrival_status == "update"
        assert first_stop.arrival_delay == timedelta(minutes=1)
        assert first_stop.departure_status == "update"
        assert first_stop.departure_delay == timedelta(minutes=1)
        assert first_stop.message is None

        second_stop = trip_updates[0].stop_time_updates[1]
        assert second_stop.stop_id == "StopR2"
        assert second_stop.arrival_status == "update"
        assert second_stop.arrival_delay == timedelta(minutes=1)
        assert second_stop.departure_status == "update"
        assert second_stop.departure_delay == timedelta(minutes=1)
        assert second_stop.message is None

        second_stop = trip_updates[0].stop_time_updates[2]
        assert second_stop.stop_id == "StopR2-bis"
        assert second_stop.arrival_status == "update"
        assert second_stop.arrival_delay == timedelta(minutes=2, seconds=30)
        assert second_stop.departure_status == "update"
        assert second_stop.departure_delay == timedelta(minutes=2, seconds=30)
        assert second_stop.message is None

        third_stop = trip_updates[0].stop_time_updates[3]
        assert third_stop.stop_id == "StopR3"
        assert third_stop.arrival_status == "update"
        assert third_stop.arrival_delay == timedelta(minutes=3)
        assert third_stop.departure_status == "update"
        assert third_stop.departure_delay == timedelta(minutes=3)
        assert third_stop.message is None

        fourth_stop = trip_updates[0].stop_time_updates[4]
        assert fourth_stop.stop_id == "StopR4"
        assert fourth_stop.arrival_status == "update"
        assert fourth_stop.arrival_delay == timedelta(minutes=4)
        assert fourth_stop.departure_status == "update"
        assert fourth_stop.departure_delay == timedelta(minutes=4)
        assert fourth_stop.message is None

        feed = convert_to_gtfsrt(trip_updates)
        assert feed.entity[0].trip_update.trip.start_date == "20120615"  # must be UTC start date


def test_gtfs_rt_pass_midnight_utc(pass_midnight_utc_gtfs_rt_data, mock_rabbitmq):
    """
    test the gtfs-rt post with a pass-midnight UTC gtfs-rt

    we have realtime data on all 5 stops

    after the merge, we should have 5 stops properly delayed
    """
    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=pass_midnight_utc_gtfs_rt_data.SerializeToString())
    assert resp.status_code == 200

    with app.app_context():
        assert len(RealTimeUpdate.query.all()) == 1
        assert len(TripUpdate.query.all()) == 1
        assert len(StopTimeUpdate.query.all()) == 5
        assert RealTimeUpdate.query.first().status == "OK"

        trip_update = TripUpdate.find_by_dated_vj("R:vj1", datetime.datetime(2012, 6, 15, 23, 30))

        assert trip_update

        assert trip_update.vj.get_start_timestamp() == datetime.datetime(2012, 6, 15, 23, 30)

        # using navitia's time in UTC
        first_stop = trip_update.stop_time_updates[0]
        assert first_stop.stop_id == "StopR1"
        assert first_stop.arrival_status == "update"
        assert first_stop.arrival_delay == timedelta(minutes=1)
        # 23:30 in UTC + 1minute of delay
        assert first_stop.arrival == datetime.datetime(2012, 6, 15, 23, 31)
        assert first_stop.departure_delay == timedelta(minutes=1)
        assert first_stop.departure_status == "update"
        assert first_stop.departure == datetime.datetime(2012, 6, 15, 23, 31)
        assert first_stop.message is None

        second_stop = trip_update.stop_time_updates[1]
        assert second_stop.stop_id == "StopR2"
        assert second_stop.arrival_status == "update"
        assert second_stop.arrival == datetime.datetime(2012, 6, 16, 0, 0)
        assert second_stop.arrival_delay == timedelta(minutes=1)
        assert second_stop.departure == datetime.datetime(2012, 6, 16, 0, 1)
        assert second_stop.departure_delay == timedelta(minutes=1)
        assert second_stop.departure_status == "update"
        assert second_stop.message is None

        second_stop = trip_update.stop_time_updates[2]
        assert second_stop.stop_id == "StopR2-bis"
        assert second_stop.arrival_status == "update"
        assert second_stop.arrival == datetime.datetime(2012, 6, 16, 0, 2, 30)
        assert second_stop.arrival_delay == timedelta(minutes=2, seconds=30)
        assert second_stop.departure == datetime.datetime(2012, 6, 16, 0, 2, 30)
        assert second_stop.departure_delay == timedelta(minutes=2, seconds=30)
        assert second_stop.departure_status == "update"
        assert second_stop.message is None

        third_stop = trip_update.stop_time_updates[3]
        assert third_stop.stop_id == "StopR3"
        assert third_stop.arrival_status == "update"
        assert third_stop.arrival_delay == timedelta(minutes=3)
        assert third_stop.arrival == datetime.datetime(2012, 6, 16, 0, 3)
        assert third_stop.departure_delay == timedelta(minutes=3)
        assert third_stop.departure_status == "update"
        assert third_stop.departure == datetime.datetime(2012, 6, 16, 0, 4)
        assert third_stop.message is None

        fourth_stop = trip_update.stop_time_updates[4]
        assert fourth_stop.stop_id == "StopR4"
        assert fourth_stop.arrival_status == "update"
        assert fourth_stop.arrival_delay == timedelta(minutes=4)
        assert fourth_stop.arrival == datetime.datetime(2012, 6, 16, 0, 34)
        assert fourth_stop.departure_delay == timedelta(minutes=4)
        assert fourth_stop.departure_status == "update"
        assert fourth_stop.departure == datetime.datetime(2012, 6, 16, 0, 34)
        assert fourth_stop.message is None


@pytest.fixture()
def partial_update_gtfs_rt_data_1():
    feed = gtfs_realtime_pb2.FeedMessage()

    feed.header.gtfs_realtime_version = "1.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = to_posix_time(datetime.datetime(year=2012, month=6, day=15, hour=15))

    entity = feed.entity.add()
    entity.id = "bob"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "Code-R-vj1"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 60
    stu.stop_sequence = 2
    stu.stop_id = "Code-StopR2"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.stop_sequence = 3
    stu.stop_id = "Code-StopR3"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.stop_sequence = 4
    stu.stop_id = "Code-StopR4"

    return feed


@pytest.fixture()
def partial_update_gtfs_rt_data_2():
    """
    This fixture is almost the same as partial_update_gtfs_rt_data_1
    It's modifying trip Code-R-vj1: StopR2 and StopR4 are delayed, StopR3 on time (no info on StopR1)
    Based on the previous one, we add one more stop_time_update
    """
    feed = gtfs_realtime_pb2.FeedMessage()

    feed.header.gtfs_realtime_version = "1.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = to_posix_time(datetime.datetime(year=2012, month=6, day=15, hour=15))

    entity = feed.entity.add()
    entity.id = "bob"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "Code-R-vj1"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 60
    stu.stop_sequence = 2
    stu.stop_id = "Code-StopR2"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.stop_sequence = 3
    stu.stop_id = "Code-StopR3"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 180
    stu.stop_sequence = 4
    stu.stop_id = "Code-StopR4"

    return feed


@pytest.fixture()
def partial_update_gtfs_rt_data_3():
    """
    This fixture is almost the same as partial_update_gtfs_rt_data_2
    We add a new trip_update
    """
    feed = gtfs_realtime_pb2.FeedMessage()

    feed.header.gtfs_realtime_version = "1.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = to_posix_time(datetime.datetime(year=2012, month=6, day=15, hour=15))

    entity = feed.entity.add()
    entity.id = "bob"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "Code-R-vj1"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 60
    stu.stop_sequence = 2
    stu.stop_id = "Code-StopR2"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.stop_sequence = 3
    stu.stop_id = "Code-StopR3"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 180
    stu.stop_sequence = 4
    stu.stop_id = "Code-StopR4"

    # Another trip update
    entity = feed.entity.add()
    entity.id = "bob"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "Code-R-vj2"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 60
    stu.departure.delay = 60
    stu.stop_sequence = 1
    stu.stop_id = "Code-StopR1"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.departure.delay = 0
    stu.stop_sequence = 2
    stu.stop_id = "Code-StopR2"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.departure.delay = 0
    stu.stop_sequence = 3
    stu.stop_id = "Code-StopR3"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.departure.delay = 0
    stu.stop_sequence = 4
    stu.stop_id = "Code-StopR4"

    return feed


@pytest.fixture()
def partial_update_gtfs_rt_code_r_jv1_last_stop_normal():
    """
    This fixture is modifying trip Code-R-vj1: StopR4 is back to normal (no info on others)
    """
    feed = gtfs_realtime_pb2.FeedMessage()

    feed.header.gtfs_realtime_version = "1.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = to_posix_time(datetime.datetime(year=2012, month=6, day=15, hour=15))

    entity = feed.entity.add()
    entity.id = "bob"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "Code-R-vj1"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.stop_sequence = 4
    stu.stop_id = "Code-StopR4"

    return feed


def test_gtfs_rt_partial_update_same_feed(partial_update_gtfs_rt_data_1):
    """
    In this test, we will send the same gtfs-rt twice, the second sending should not create neither
    new trip updates nor new stop time updates
    """
    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=partial_update_gtfs_rt_data_1.SerializeToString())
    assert resp.status_code == 200

    def check(nb_rt_update):
        with app.app_context():
            assert len(RealTimeUpdate.query.all()) == nb_rt_update
            assert len(TripUpdate.query.all()) == 1
            assert len(StopTimeUpdate.query.all()) == 4

            trip_update = TripUpdate.query.first()
            assert trip_update.stop_time_updates[0].arrival_delay.seconds == 0
            assert trip_update.stop_time_updates[1].arrival_delay.seconds == 60
            assert trip_update.stop_time_updates[2].arrival_delay.seconds == 0
            assert trip_update.stop_time_updates[3].arrival_delay.seconds == 0
            # since the second real_time_update is the same as the first one,
            # the second one won't have an effect on existing trip update,
            # so the length is 1
            assert len(trip_update.real_time_updates) == 1

            if nb_rt_update == 2:
                last_real_time_update = RealTimeUpdate.query.order_by(RealTimeUpdate.created_at.desc()).first()
                assert last_real_time_update.status == "KO"
                assert last_real_time_update.error == "No new information destinated to navitia for this gtfs-rt"

    check(nb_rt_update=1)

    # Now we apply exactly the same gtfs-rt, the new gtfs-rt will be save into the db,
    # which increment the nb of RealTimeUpdate, but the rest remains the same that means....
    # 1. There will not be any trip_updates in the data base related to the last real_time_update
    # 2. with real_time_update.status = 'KO' and real_time_update.error = 'No new Information...'
    resp = tester.post("/gtfs_rt", data=partial_update_gtfs_rt_data_1.SerializeToString())
    assert resp.status_code == 200

    check(nb_rt_update=2)


def test_gtfs_rt_partial_update_diff_feed_1(partial_update_gtfs_rt_data_1, partial_update_gtfs_rt_data_2):
    """
    In this test, we will send the two different gtfs-rt
    """
    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=partial_update_gtfs_rt_data_1.SerializeToString())
    assert resp.status_code == 200

    with app.app_context():
        assert len(RealTimeUpdate.query.all()) == 1
        assert len(TripUpdate.query.all()) == 1
        assert len(StopTimeUpdate.query.all()) == 4

        trip_update = TripUpdate.query.first()
        assert trip_update.stop_time_updates[0].arrival_delay.seconds == 0
        assert trip_update.stop_time_updates[1].arrival_delay.seconds == 60
        assert trip_update.stop_time_updates[2].arrival_delay.seconds == 0
        assert trip_update.stop_time_updates[3].arrival_delay.seconds == 0
        assert len(trip_update.real_time_updates) == 1

    # Now we apply another gtfs-rt, the new gtfs-rt will be save into the db and
    # increments the nb of real_time_updates
    resp = tester.post("/gtfs_rt", data=partial_update_gtfs_rt_data_2.SerializeToString())
    assert resp.status_code == 200
    with app.app_context():
        assert len(RealTimeUpdate.query.all()) == 2
        assert len(TripUpdate.query.all()) == 1
        assert len(StopTimeUpdate.query.all()) == 4

        trip_update = TripUpdate.query.first()
        assert trip_update.stop_time_updates[0].arrival_delay.seconds == 0
        assert trip_update.stop_time_updates[1].arrival_delay.seconds == 60
        assert trip_update.stop_time_updates[2].arrival_delay.seconds == 0
        assert trip_update.stop_time_updates[3].arrival_delay.seconds == 180
        assert len(trip_update.real_time_updates) == 2


def test_gtfs_rt_partial_update_diff_feed_2(partial_update_gtfs_rt_data_2, partial_update_gtfs_rt_data_3):
    """
    In this test, we send a gtfs-rt containing only one trip_update, the send a gtfs-rt
    containing two trip_updates
    """
    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=partial_update_gtfs_rt_data_2.SerializeToString())
    assert resp.status_code == 200

    with app.app_context():
        assert len(RealTimeUpdate.query.all()) == 1
        assert len(TripUpdate.query.all()) == 1
        assert len(StopTimeUpdate.query.all()) == 4

        trip_update = TripUpdate.query.first()
        first_trip_update_db_id = trip_update.vj_id
        assert trip_update.stop_time_updates[0].arrival_delay.seconds == 0
        assert trip_update.stop_time_updates[1].arrival_delay.seconds == 60
        assert trip_update.stop_time_updates[2].arrival_delay.seconds == 0
        assert trip_update.stop_time_updates[3].arrival_delay.seconds == 180
        assert len(trip_update.real_time_updates) == 1

    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=partial_update_gtfs_rt_data_3.SerializeToString())
    assert resp.status_code == 200

    with app.app_context():
        assert len(RealTimeUpdate.query.all()) == 2
        trip_updates = TripUpdate.query.all()
        assert len(trip_updates) == 2
        assert len(StopTimeUpdate.query.all()) == 8

        for trip_update in trip_updates:
            if trip_update.vj_id == first_trip_update_db_id:
                assert trip_update.stop_time_updates[0].arrival_delay.seconds == 0
                assert trip_update.stop_time_updates[1].arrival_delay.seconds == 60
                assert trip_update.stop_time_updates[2].arrival_delay.seconds == 0
                assert trip_update.stop_time_updates[3].arrival_delay.seconds == 180
                assert len(trip_update.real_time_updates) == 1
            else:
                assert trip_update.stop_time_updates[0].arrival_delay.seconds == 60
                assert trip_update.stop_time_updates[1].arrival_delay.seconds == 0
                assert trip_update.stop_time_updates[2].arrival_delay.seconds == 0
                assert trip_update.stop_time_updates[3].arrival_delay.seconds == 0
                assert len(trip_update.real_time_updates) == 1


def test_gtfs_rt_partial_update_last_stop_back_normal(
    partial_update_gtfs_rt_data_2, partial_update_gtfs_rt_code_r_jv1_last_stop_normal
):
    """
    In this test, we send a gtfs-rt containing only one trip_update (delay on R2 and R4),
    then send a gtfs-rt with just back-to-normal info on R4.
    Currently, this second feed is seen as back-to-normal on all stops, as GTFS-RT feed is currently
    supposed to be complete (stops not provided are served on time)
    """
    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=partial_update_gtfs_rt_data_2.SerializeToString())
    assert resp.status_code == 200

    with app.app_context():
        assert len(RealTimeUpdate.query.all()) == 1
        assert len(TripUpdate.query.all()) == 1
        assert len(StopTimeUpdate.query.all()) == 4

        trip_update = TripUpdate.query.first()
        assert trip_update.stop_time_updates[0].arrival_delay.seconds == 0
        assert trip_update.stop_time_updates[1].arrival_delay.seconds == 60
        assert trip_update.stop_time_updates[2].arrival_delay.seconds == 0
        assert trip_update.stop_time_updates[3].arrival_delay.seconds == 180
        assert len(trip_update.real_time_updates) == 1
        assert trip_update.effect == TripEffect.SIGNIFICANT_DELAYS.name

    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=partial_update_gtfs_rt_code_r_jv1_last_stop_normal.SerializeToString())
    assert resp.status_code == 200

    with app.app_context():
        assert len(RealTimeUpdate.query.all()) == 2
        assert len(TripUpdate.query.all()) == 1
        assert len(StopTimeUpdate.query.all()) == 4

        trip_update = TripUpdate.query.first()
        assert trip_update.stop_time_updates[0].arrival_delay.seconds == 0

        # from previous feed: R2 was delayed. It is not the case anymore as current implementation considers
        # GTFS-RT feeds on a trip as complete, and no info on stop means it's on time.
        # (if stop R2 is delayed, trip.effect below should match that)
        assert trip_update.stop_time_updates[1].arrival_delay.seconds == 0

        assert trip_update.stop_time_updates[2].arrival_delay.seconds == 0
        assert trip_update.stop_time_updates[3].arrival_delay.seconds == 0
        assert len(trip_update.real_time_updates) == 2

        # check that effect matches the absence or presence of delay
        if all(stu.arrival_delay.seconds == 0 for stu in trip_update.stop_time_updates):
            assert trip_update.effect == TripEffect.UNKNOWN_EFFECT.name
        else:
            assert trip_update.effect == TripEffect.SIGNIFICANT_DELAYS.name


"""
vj.stop_times:  StopR1	StopR2	StopR3	StopR2	StopR4
order:          0       1       2       3       4

gtfs-rt.stop:   StopR1  StopR2  StopR3  StopR2  StopR4
stop_sequence:  1       2       3       4       5
Status:         Delay   Delay   Delay   None    None

Since the gtfs-rt.stop list is a strict ending sublist of vj.stop_times we merge
informations of each trip update stop with that of navitia vj
Stop-Match:     StopR1	StopR2	StopR3	StopR2	StopR4
order:          0       1       2       3       4
status:         Delay   Delay   Delay   None    None
"""


@pytest.fixture()
def lollipop_gtfs_rt_data():
    feed = gtfs_realtime_pb2.FeedMessage()

    feed.header.gtfs_realtime_version = "1.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = to_posix_time(datetime.datetime(year=2012, month=6, day=15, hour=15))

    entity = feed.entity.add()
    entity.id = "bob"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "Code-lollipop"

    # arrival and departure in vehiclejourney 100000
    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 60
    stu.departure.delay = 60
    stu.stop_sequence = 1
    stu.stop_id = "Code-StopR1"

    # arrival and departure in vehiclejourney 101000
    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 120
    stu.departure.delay = 120
    stu.stop_sequence = 2
    stu.stop_id = "Code-StopR2"

    # arrival and departure in vehiclejourney 102000
    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 60
    stu.departure.delay = 60
    stu.stop_sequence = 3
    stu.stop_id = "Code-StopR3"

    # arrival and departure in vehiclejourney 103000
    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.departure.delay = 0
    stu.stop_sequence = 4
    stu.stop_id = "Code-StopR2"

    # arrival and departure in vehiclejourney 104000
    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.departure.delay = 0
    stu.stop_sequence = 5
    stu.stop_id = "Code-StopR4"

    return feed


def test_gtfs_lollipop_model_builder(lollipop_gtfs_rt_data):
    """
    test the model builder with a lollipop gtfs-rt
    """
    with app.app_context():
        data = ""
        rt_update = RealTimeUpdate(data, connector="gtfs-rt", contributor="realtime.sherbrooke")
        trip_updates = gtfs_rt.KirinModelBuilder(dumb_nav_wrapper(), contributor="realtime.sherbrooke").build(
            rt_update, lollipop_gtfs_rt_data
        )

        # we associate the trip_update manually for sqlalchemy to make the links
        rt_update.trip_updates = trip_updates
        db.session.add(rt_update)
        db.session.commit()

        assert len(trip_updates) == 1
        assert len(trip_updates[0].stop_time_updates) == 5

        first_stop = trip_updates[0].stop_time_updates[0]
        assert first_stop.stop_id == "StopR1"
        assert first_stop.arrival_status == "update"
        assert first_stop.arrival_delay == timedelta(minutes=1)
        assert first_stop.departure_status == "update"
        assert first_stop.departure_delay == timedelta(minutes=1)
        assert first_stop.message is None

        second_stop = trip_updates[0].stop_time_updates[1]
        assert second_stop.stop_id == "StopR2"
        assert second_stop.arrival_status == "update"
        assert second_stop.arrival_delay == timedelta(minutes=2)
        assert second_stop.departure_status == "update"
        assert second_stop.departure_delay == timedelta(minutes=2)
        assert second_stop.message is None

        third_stop = trip_updates[0].stop_time_updates[2]
        assert third_stop.stop_id == "StopR3"
        assert third_stop.arrival_status == "update"
        assert third_stop.arrival_delay == timedelta(minutes=1)
        assert third_stop.departure_status == "update"
        assert third_stop.departure_delay == timedelta(minutes=1)
        assert third_stop.message is None

        fourth_stop = trip_updates[0].stop_time_updates[3]
        assert fourth_stop.stop_id == "StopR2"
        assert fourth_stop.arrival_status == "none"
        assert fourth_stop.arrival_delay is None
        assert fourth_stop.departure_status == "none"
        assert fourth_stop.departure_delay is None
        assert fourth_stop.message is None

        fifth_stop = trip_updates[0].stop_time_updates[4]
        assert fifth_stop.stop_id == "StopR4"
        assert fifth_stop.arrival_status == "none"
        assert fifth_stop.arrival_delay is None
        assert fifth_stop.departure_status == "none"
        assert fifth_stop.departure_delay is None
        assert fifth_stop.message is None

        feed = convert_to_gtfsrt(trip_updates)
        assert feed.entity[0].trip_update.trip.start_date == "20120615"


"""
vj.stop_times:  StopR1	StopR2	StopR3	StopR4	    StopR5	StopR6
order:          0       1       2       3           4       5

gtfs-rt.stop:           StopR2  StopR3  Stop-RT-1   StopR4  StopR6
stop_sequence:          2       3       4           5       6

Since the gtfs-rt.stop list is a strict ending sublist of vj.stop_times, we reject this trip update.
Stop-Match:     None
"""


@pytest.fixture()
def bad_ordered_gtfs_rt_data():
    feed = gtfs_realtime_pb2.FeedMessage()

    feed.header.gtfs_realtime_version = "1.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = to_posix_time(datetime.datetime(year=2012, month=6, day=15, hour=15))

    entity = feed.entity.add()
    entity.id = "bob"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "Code-orders"

    # arrival and departure in vehiclejourney 101000
    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 60
    stu.departure.delay = 60
    stu.stop_sequence = 2
    stu.stop_id = "Code-StopR2"

    # arrival and departure in vehiclejourney 102000
    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 120
    stu.departure.delay = 120
    stu.stop_sequence = 3
    stu.stop_id = "Code-StopR3"

    # stop absent in vehiclejourney and will be rejected
    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 120
    stu.departure.delay = 120
    stu.stop_sequence = 4
    stu.stop_id = "Code-Stop-RT-1"

    # stop also present in vehiclejourney but since its order doesn't match with stop_sequence, will be rejected.
    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 120
    stu.departure.delay = 120
    stu.stop_sequence = 5
    stu.stop_id = "Code-StopR4"

    # arrival and departure in vehiclejourney 105000
    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 60
    stu.departure.delay = 60
    stu.stop_sequence = 6
    stu.stop_id = "Code-StopR6"

    return feed


def test_gtfs_bad_order_model_builder(bad_ordered_gtfs_rt_data):
    """
    test the model builder with stops absent or not matching order in gtfs-rt
    """
    with app.app_context():
        data = ""
        rt_update = RealTimeUpdate(data, connector="gtfs-rt", contributor="realtime.sherbrooke")
        trip_updates = gtfs_rt.KirinModelBuilder(dumb_nav_wrapper(), contributor="realtime.sherbrooke").build(
            rt_update, bad_ordered_gtfs_rt_data
        )

        # we associate the trip_update manually for sqlalchemy to make the links
        rt_update.trip_updates = trip_updates
        db.session.add(rt_update)
        db.session.commit()

        assert len(trip_updates) == 0
        assert len(RealTimeUpdate.query.all()) == 1
        assert RealTimeUpdate.query.first().status == "KO"
        assert RealTimeUpdate.query.first().error == "No information for this gtfs-rt with timestamp: 1339772400"


def test_gtfs_bad_order_model_builder_with_post(bad_ordered_gtfs_rt_data):
    """
    test the model builder with stops absent or not matching order in gtfs-rt

    we have realtime data with 6 stops and vehicle journey with 6 stops

    Since two lists above do not match from the last element towards left, we reject this trip update

    """
    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=bad_ordered_gtfs_rt_data.SerializeToString())
    assert resp.status_code == 200

    def check(nb_rt_update):
        with app.app_context():
            assert len(RealTimeUpdate.query.all()) == nb_rt_update
            assert len(TripUpdate.query.all()) == 0
            assert RealTimeUpdate.query.first().status == "KO"
            assert (
                RealTimeUpdate.query.first().error
                == "No information for this gtfs-rt with timestamp: 1339772400"
            )

    check(nb_rt_update=1)

    # Now we apply exactly the same gtfs-rt, the new gtfs-rt will be saved into the db,
    # but the trip update won't be saved
    resp = tester.post("/gtfs_rt", data=bad_ordered_gtfs_rt_data.SerializeToString())
    assert resp.status_code == 200
    check(nb_rt_update=2)


def test_gtfs_lollipop_model_builder_with_post(lollipop_gtfs_rt_data):
    """
    test the model builder with stops served more than once

    we have realtime data with 4 stops with stop StopR2 served twice

    Since the gtfs-rt.stop list is a strict ending sublist of vj.stop_times we merge
    """
    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=lollipop_gtfs_rt_data.SerializeToString())
    assert resp.status_code == 200

    def check(nb_rt_update):
        with app.app_context():
            assert len(RealTimeUpdate.query.all()) == nb_rt_update
            assert RealTimeUpdate.query.first().status == "OK"
            assert len(TripUpdate.query.all()) == 1
            assert len(StopTimeUpdate.query.all()) == 5

            trip_update = TripUpdate.find_by_dated_vj("R:vj1", datetime.datetime(2012, 6, 15, 14, 00))

            assert trip_update

            assert trip_update.vj.get_start_timestamp() == datetime.datetime(2012, 6, 15, 14, 00)

            first_stop = trip_update.stop_time_updates[0]
            assert first_stop.stop_id == "StopR1"
            assert first_stop.arrival_status == "update"
            assert first_stop.arrival_delay == timedelta(minutes=1)
            assert first_stop.arrival == datetime.datetime(2012, 6, 15, 14, 1)
            assert first_stop.departure_delay == timedelta(minutes=1)
            assert first_stop.departure_status == "update"
            assert first_stop.departure == datetime.datetime(2012, 6, 15, 14, 1)
            assert first_stop.message is None

            second_stop = trip_update.stop_time_updates[1]
            assert second_stop.stop_id == "StopR2"
            assert second_stop.arrival_status == "update"
            assert second_stop.arrival == datetime.datetime(2012, 6, 15, 14, 12)
            assert second_stop.arrival_delay == timedelta(minutes=2)
            assert second_stop.departure == datetime.datetime(2012, 6, 15, 14, 12)
            assert second_stop.departure_delay == timedelta(minutes=2)
            assert second_stop.departure_status == "update"
            assert second_stop.message is None

            third_stop = trip_update.stop_time_updates[2]
            assert third_stop.stop_id == "StopR3"
            assert third_stop.arrival_status == "update"
            assert third_stop.arrival == datetime.datetime(2012, 6, 15, 14, 21)
            assert third_stop.arrival_delay == timedelta(minutes=1)
            assert third_stop.departure == datetime.datetime(2012, 6, 15, 14, 21)
            assert third_stop.departure_delay == timedelta(minutes=1)
            assert third_stop.departure_status == "update"
            assert third_stop.message is None

            fourth_stop = trip_update.stop_time_updates[3]
            assert fourth_stop.stop_id == "StopR2"
            assert fourth_stop.arrival_status == "none"
            assert fourth_stop.arrival_delay == timedelta(0)
            assert fourth_stop.arrival == datetime.datetime(2012, 6, 15, 14, 30)
            assert fourth_stop.departure_delay == timedelta(0)
            assert fourth_stop.departure_status == "none"
            assert fourth_stop.departure == datetime.datetime(2012, 6, 15, 14, 30)
            assert fourth_stop.message is None

            fifth_stop = trip_update.stop_time_updates[4]
            assert fifth_stop.stop_id == "StopR4"
            assert fifth_stop.arrival_status == "none"
            assert fifth_stop.arrival_delay == timedelta(0)
            assert fifth_stop.arrival == datetime.datetime(2012, 6, 15, 14, 40)
            assert fifth_stop.departure_delay == timedelta(0)
            assert fifth_stop.departure_status == "none"
            assert fifth_stop.departure == datetime.datetime(2012, 6, 15, 14, 40)
            assert fifth_stop.message is None

    check(nb_rt_update=1)

    # Now we apply exactly the same gtfs-rt, the new gtfs-rt will be save into the db,
    # which increment the nb of RealTimeUpdate, but every else remains the same
    resp = tester.post("/gtfs_rt", data=lollipop_gtfs_rt_data.SerializeToString())
    assert resp.status_code == 200
    check(nb_rt_update=2)


"""
vj.stop_times:  StopR1	StopR2	StopR3	StopR2	StopR4
order:          0       1       2       3       4

gtfs-rt.stop:                           StopR2  StopR4
stop_sequence:                          4       5
Status:         None    None    None    Delay   None

Since the gtfs-rt.stop list is a strict ending sublist of vj.stop_times we merge
informations of each trip update stop with that of navitia vj
Stop-Match:     StopR1	StopR2	StopR3	StopR2	StopR4
order:          0       1       2       3       4
status:         None    None    None    Delay   None
"""


@pytest.fixture()
def lollipop_gtfs_rt_from_second_passage_data():
    feed = gtfs_realtime_pb2.FeedMessage()

    feed.header.gtfs_realtime_version = "1.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = to_posix_time(datetime.datetime(year=2012, month=6, day=15, hour=15))

    entity = feed.entity.add()
    entity.id = "bob"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "Code-lollipop"

    # arrival and departure in vehiclejourney 103000
    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 60
    stu.departure.delay = 60
    stu.stop_sequence = 4
    stu.stop_id = "Code-StopR2"

    # arrival and departure in vehiclejourney 104000
    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.departure.delay = 0
    stu.stop_sequence = 5
    stu.stop_id = "Code-StopR4"

    return feed


def test_gtfs_lollipop_for_second_passage_model_builder(lollipop_gtfs_rt_from_second_passage_data):
    """
    test the model builder with a lollipop gtfs-rt
    """
    with app.app_context():
        data = ""
        rt_update = RealTimeUpdate(data, connector="gtfs-rt", contributor="realtime.sherbrooke")
        trip_updates = gtfs_rt.KirinModelBuilder(dumb_nav_wrapper(), contributor="realtime.sherbrooke").build(
            rt_update, lollipop_gtfs_rt_from_second_passage_data
        )

        # we associate the trip_update manually for sqlalchemy to make the links
        rt_update.trip_updates = trip_updates
        db.session.add(rt_update)
        db.session.commit()

        assert len(trip_updates) == 1
        assert len(trip_updates[0].stop_time_updates) == 5

        first_stop = trip_updates[0].stop_time_updates[0]
        assert first_stop.stop_id == "StopR1"
        assert first_stop.arrival_status == "none"
        assert first_stop.arrival_delay is None
        assert first_stop.departure_status == "none"
        assert first_stop.departure_delay is None
        assert first_stop.message is None

        second_stop = trip_updates[0].stop_time_updates[1]
        assert second_stop.stop_id == "StopR2"
        assert second_stop.arrival_status == "none"
        assert second_stop.arrival_delay is None
        assert second_stop.departure_status == "none"
        assert second_stop.departure_delay is None
        assert second_stop.message is None

        third_stop = trip_updates[0].stop_time_updates[2]
        assert third_stop.stop_id == "StopR3"
        assert third_stop.arrival_status == "none"
        assert third_stop.arrival_delay is None
        assert third_stop.departure_status == "none"
        assert third_stop.departure_delay is None
        assert third_stop.message is None

        fourth_stop = trip_updates[0].stop_time_updates[3]
        assert fourth_stop.stop_id == "StopR2"
        assert fourth_stop.arrival_status == "update"
        assert fourth_stop.arrival_delay == timedelta(minutes=1)
        assert fourth_stop.departure_status == "update"
        assert fourth_stop.departure_delay == timedelta(minutes=1)
        assert fourth_stop.message is None

        fifth_stop = trip_updates[0].stop_time_updates[4]
        assert fifth_stop.stop_id == "StopR4"
        assert fifth_stop.arrival_status == "none"
        assert fifth_stop.arrival_delay is None
        assert fifth_stop.departure_status == "none"
        assert fifth_stop.departure_delay is None
        assert fifth_stop.message is None

        feed = convert_to_gtfsrt(trip_updates)
        assert feed.entity[0].trip_update.trip.start_date == "20120615"


def test_gtfs_lollipop_with_second_passage_model_builder_with_post(lollipop_gtfs_rt_from_second_passage_data):
    """
    test the model builder with stops served more than once

    we have realtime data with 2 stops from second passage of StopR2

    """
    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=lollipop_gtfs_rt_from_second_passage_data.SerializeToString())
    assert resp.status_code == 200

    def check(nb_rt_update):
        with app.app_context():
            assert len(RealTimeUpdate.query.all()) == nb_rt_update
            assert len(TripUpdate.query.all()) == 1
            assert len(StopTimeUpdate.query.all()) == 5

            trip_update = TripUpdate.find_by_dated_vj("R:vj1", datetime.datetime(2012, 6, 15, 14, 00))

            assert trip_update

            assert trip_update.vj.get_start_timestamp() == datetime.datetime(2012, 6, 15, 14, 00)

            first_stop = trip_update.stop_time_updates[0]
            assert first_stop.stop_id == "StopR1"
            assert first_stop.arrival_status == "none"
            assert first_stop.arrival_delay == timedelta(0)
            assert first_stop.arrival == datetime.datetime(2012, 6, 15, 14, 00)
            assert first_stop.departure_delay == timedelta(0)
            assert first_stop.departure_status == "none"
            assert first_stop.departure == datetime.datetime(2012, 6, 15, 14, 00)
            assert first_stop.message is None

            second_stop = trip_update.stop_time_updates[1]
            assert second_stop.stop_id == "StopR2"
            assert second_stop.arrival_status == "none"
            assert second_stop.arrival == datetime.datetime(2012, 6, 15, 14, 10)
            assert second_stop.arrival_delay == timedelta(0)
            assert second_stop.departure == datetime.datetime(2012, 6, 15, 14, 10)
            assert second_stop.departure_delay == timedelta(0)
            assert second_stop.departure_status == "none"
            assert second_stop.message is None

            third_stop = trip_update.stop_time_updates[2]
            assert third_stop.stop_id == "StopR3"
            assert third_stop.arrival_status == "none"
            assert third_stop.arrival == datetime.datetime(2012, 6, 15, 14, 20)
            assert third_stop.arrival_delay == timedelta(0)
            assert third_stop.departure == datetime.datetime(2012, 6, 15, 14, 20)
            assert third_stop.departure_delay == timedelta(0)
            assert third_stop.departure_status == "none"
            assert third_stop.message is None

            fourth_stop = trip_update.stop_time_updates[3]
            assert fourth_stop.stop_id == "StopR2"
            assert fourth_stop.arrival_status == "update"
            assert fourth_stop.arrival_delay == timedelta(minutes=1)
            assert fourth_stop.arrival == datetime.datetime(2012, 6, 15, 14, 31)
            assert fourth_stop.departure_delay == timedelta(minutes=1)
            assert fourth_stop.departure_status == "update"
            assert fourth_stop.departure == datetime.datetime(2012, 6, 15, 14, 31)
            assert fourth_stop.message is None

            fifth_stop = trip_update.stop_time_updates[4]
            assert fifth_stop.stop_id == "StopR4"
            assert fifth_stop.arrival_status == "none"
            assert fifth_stop.arrival_delay == timedelta(0)
            assert fifth_stop.arrival == datetime.datetime(2012, 6, 15, 14, 40)
            assert fifth_stop.departure_delay == timedelta(0)
            assert fifth_stop.departure_status == "none"
            assert fifth_stop.departure == datetime.datetime(2012, 6, 15, 14, 40)
            assert fifth_stop.message is None

    check(nb_rt_update=1)

    # Now we apply exactly the same gtfs-rt, the new gtfs-rt will be save into the db,
    # which increment the nb of RealTimeUpdate, but every else remains the same
    resp = tester.post("/gtfs_rt", data=lollipop_gtfs_rt_from_second_passage_data.SerializeToString())
    assert resp.status_code == 200
    check(nb_rt_update=2)


@pytest.fixture()
def gtfs_rt_data_with_more_stops():
    feed = gtfs_realtime_pb2.FeedMessage()

    feed.header.gtfs_realtime_version = "1.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = to_posix_time(datetime.datetime(year=2012, month=6, day=15, hour=15))

    entity = feed.entity.add()
    entity.id = "bob"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "Code-R-vj1"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.stop_sequence = 0
    stu.stop_id = "Code-StopR0"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.stop_sequence = 1
    stu.stop_id = "Code-StopR1"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 60
    stu.stop_sequence = 2
    stu.stop_id = "Code-StopR2"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.stop_sequence = 3
    stu.stop_id = "Code-StopR3"

    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 180
    stu.stop_sequence = 4
    stu.stop_id = "Code-StopR4"

    return feed


def test_gtfs_more_stops_model_builder(gtfs_rt_data_with_more_stops):
    """
    test the model builder with gtfs-rt having more stops than in vj
    """
    with app.app_context():
        data = ""
        rt_update = RealTimeUpdate(data, connector="gtfs-rt", contributor="realtime.sherbrooke")
        trip_updates = gtfs_rt.KirinModelBuilder(dumb_nav_wrapper(), contributor="realtime.sherbrooke").build(
            rt_update, gtfs_rt_data_with_more_stops
        )

        rt_update.trip_updates = trip_updates
        db.session.add(rt_update)
        db.session.commit()

        assert len(trip_updates) == 0


"""
This error message occurred many times a day for the same vehicle_journey with feed timestamp between 1h and 2h UTC
@timestamp: 2017-12-12T01:58:44.000Z -> localtime : 2017-12-11T20:58:44.000
impossible to calculate the circulate date (local) of vj: vehicle_journey:STS:462247-1
Analysis:
1. Concerns gtfs-rt which arrives between 20h and 21h localtime of the day before (1h and 2h UTC) in the morning
with a vehicle_journey having first stop_time at mid-night localtime (5h UTC)

2. since = 20171211T220000Z , until = 20171212T050000Z
3. The first stop_time of the vehicle_journey is at 00 00 00 localtime (05h UTC) where as the gtfs-rt arrives after
 1h UTC -> GTFS-RT has an information on a vehicle journey with departure 4 hours in the future!!!

4. Filter in the code:
 since_local = 20171211T170000 -05:00, until_local = 20171212T000000 -05:00

 since_local <= date(since_local) + 00 00 00 (first_vj_stop_time) <= until_local -> false
 20171211T170000 -05:00 <= 20171211T000000 -05:00<=  20171212T000000 -05:00

 since_local <= date(until_local) + 00 00 00 (first_vj_stop_time) <= until_local -> true
 20171211T170000 -05:00 <= 20171212T000000 -05:00 <= 20171212T000000 -05:00

 No realtime in navitia.
"""


@pytest.fixture()
def gtfs_rt_data_with_vj_starting_at_midnight():
    feed = gtfs_realtime_pb2.FeedMessage()

    feed.header.gtfs_realtime_version = "1.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = to_posix_time(datetime.datetime(year=2017, month=12, day=12, hour=1, minute=17))

    entity = feed.entity.add()
    entity.id = "bob"
    trip_update = entity.trip_update
    trip_update.trip.trip_id = "Code-midnight"

    # arrival and departure in vehiclejourney 000000
    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 0
    stu.departure.delay = 0
    stu.stop_sequence = 1
    stu.stop_id = "Code-StopR1"

    # arrival and departure in vehiclejourney 001000
    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 120
    stu.departure.delay = 120
    stu.stop_sequence = 2
    stu.stop_id = "Code-StopR2"

    # arrival and departure in vehiclejourney 002000
    stu = trip_update.stop_time_update.add()
    stu.arrival.delay = 60
    stu.departure.delay = 60
    stu.stop_sequence = 3
    stu.stop_id = "Code-StopR3"

    return feed


def test_gtfs_start_midnight_model_builder_with_post(gtfs_rt_data_with_vj_starting_at_midnight):
    """
    test the model builder with vehicle_journey having first stop_time at midnight
    """
    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=gtfs_rt_data_with_vj_starting_at_midnight.SerializeToString())
    assert resp.status_code == 200

    def check(nb_rt_update):
        with app.app_context():
            assert len(RealTimeUpdate.query.all()) == nb_rt_update
            assert len(TripUpdate.query.all()) == 1
            assert len(StopTimeUpdate.query.all()) == 3

            trip_update = TripUpdate.find_by_dated_vj("R:vj1", datetime.datetime(2017, 12, 12, 5, 0))

            assert trip_update

            assert trip_update.vj.get_start_timestamp() == datetime.datetime(2017, 12, 12, 5, 0)

            first_stop = trip_update.stop_time_updates[0]
            assert first_stop.stop_id == "StopR1"
            assert first_stop.arrival_status == "none"
            assert first_stop.arrival_delay == timedelta(0)
            assert first_stop.arrival == datetime.datetime(2017, 12, 12, 5, 0)
            assert first_stop.departure_delay == timedelta(0)
            assert first_stop.departure_status == "none"
            assert first_stop.departure == datetime.datetime(2017, 12, 12, 5, 0)
            assert first_stop.message is None

            second_stop = trip_update.stop_time_updates[1]
            assert second_stop.stop_id == "StopR2"
            assert second_stop.arrival_status == "update"
            assert second_stop.arrival == datetime.datetime(2017, 12, 12, 5, 12)
            assert second_stop.arrival_delay == timedelta(minutes=2)
            assert second_stop.departure == datetime.datetime(2017, 12, 12, 5, 12)
            assert second_stop.departure_delay == timedelta(minutes=2)
            assert second_stop.departure_status == "update"
            assert second_stop.message is None

            third_stop = trip_update.stop_time_updates[2]
            assert third_stop.stop_id == "StopR3"
            assert third_stop.arrival_status == "update"
            assert third_stop.arrival == datetime.datetime(2017, 12, 12, 5, 21)
            assert third_stop.arrival_delay == timedelta(minutes=1)
            assert third_stop.departure == datetime.datetime(2017, 12, 12, 5, 21)
            assert third_stop.departure_delay == timedelta(minutes=1)
            assert third_stop.departure_status == "update"
            assert third_stop.message is None

    check(nb_rt_update=1)


@pytest.fixture()
def gtfs_rt_data_with_vj_starting_at_midnight_utc():
    """
    Port test for start-midnight UTC also
    """
    feed = deepcopy(gtfs_rt_data_with_vj_starting_at_midnight())
    feed.header.timestamp = to_posix_time(datetime.datetime(year=2017, month=12, day=11, hour=20, minute=17))
    feed.entity[0].trip_update.trip.trip_id = "Code-midnight-UTC"

    return feed


def test_gtfs_start_midnight_utc_model_builder_with_post(gtfs_rt_data_with_vj_starting_at_midnight_utc):
    """
    test the model builder with vehicle_journey having first stop_time at midnight UTC
    """
    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=gtfs_rt_data_with_vj_starting_at_midnight_utc.SerializeToString())
    assert resp.status_code == 200

    def check(nb_rt_update):
        with app.app_context():
            assert len(RealTimeUpdate.query.all()) == nb_rt_update
            assert len(TripUpdate.query.all()) == 1
            assert len(StopTimeUpdate.query.all()) == 3

            trip_update = TripUpdate.find_by_dated_vj("R:vj1", datetime.datetime(2017, 12, 12, 00, 00))

            assert trip_update

            assert trip_update.vj.get_start_timestamp() == datetime.datetime(2017, 12, 12, 00, 00)

            first_stop = trip_update.stop_time_updates[0]
            assert first_stop.stop_id == "StopR1"
            assert first_stop.arrival_status == "none"
            assert first_stop.arrival_delay == timedelta(0)
            assert first_stop.arrival == datetime.datetime(2017, 12, 12, 00, 00)
            assert first_stop.departure_delay == timedelta(0)
            assert first_stop.departure_status == "none"
            assert first_stop.departure == datetime.datetime(2017, 12, 12, 00, 00)
            assert first_stop.message is None

            second_stop = trip_update.stop_time_updates[1]
            assert second_stop.stop_id == "StopR2"
            assert second_stop.arrival_status == "update"
            assert second_stop.arrival == datetime.datetime(2017, 12, 12, 00, 12)
            assert second_stop.arrival_delay == timedelta(minutes=2)
            assert second_stop.departure == datetime.datetime(2017, 12, 12, 00, 12)
            assert second_stop.departure_delay == timedelta(minutes=2)
            assert second_stop.departure_status == "update"
            assert second_stop.message is None

            third_stop = trip_update.stop_time_updates[2]
            assert third_stop.stop_id == "StopR3"
            assert third_stop.arrival_status == "update"
            assert third_stop.arrival == datetime.datetime(2017, 12, 12, 00, 21)
            assert third_stop.arrival_delay == timedelta(minutes=1)
            assert third_stop.departure == datetime.datetime(2017, 12, 12, 00, 21)
            assert third_stop.departure_delay == timedelta(minutes=1)
            assert third_stop.departure_status == "update"
            assert third_stop.message is None

    check(nb_rt_update=1)


def test_gtfs_rt_api_with_decode_error(basic_gtfs_rt_data):
    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=basic_gtfs_rt_data.SerializeToString() + str(">toto"))
    assert resp.status_code == 400

    def check(nb_rt_update):
        with app.app_context():
            assert len(RealTimeUpdate.query.all()) == nb_rt_update
            assert len(TripUpdate.query.all()) == 0
            assert RealTimeUpdate.query.first().status == "KO"
            assert RealTimeUpdate.query.first().error == "Decode Error"

    check(nb_rt_update=1)


def test_save_gtfs_rt_with_error():
    """
    test the function "save_gtfs_rt_with_error"
    """
    with app.app_context():
        save_gtfs_rt_with_error(
            "toto", "gtfs-rt", contributor="realtime.sherbrooke", status="KO", error="Decode Error"
        )
        assert len(RealTimeUpdate.query.all()) == 1
        assert RealTimeUpdate.query.first().status == "KO"
        assert RealTimeUpdate.query.first().error == "Decode Error"


def test_manage_db_with_http_error_without_insert():
    """
    test the function "manage_db_error" without any insert of a new gtfs-rt
    """
    with app.app_context():
        manage_db_error("toto", "gtfs-rt", contributor="realtime.sherbrooke", status="KO", error="Http Error")
        assert len(RealTimeUpdate.query.all()) == 1
        assert RealTimeUpdate.query.first().raw_data == "toto"
        assert RealTimeUpdate.query.first().status == "KO"
        assert RealTimeUpdate.query.first().error == "Http Error"

        created_at = RealTimeUpdate.query.first().created_at
        updated_at = RealTimeUpdate.query.first().updated_at
        assert updated_at > created_at

        manage_db_error("toto", "gtfs-rt", contributor="realtime.sherbrooke", status="KO", error="Http Error")
        assert len(RealTimeUpdate.query.all()) == 1
        assert RealTimeUpdate.query.first().raw_data == "toto"
        assert RealTimeUpdate.query.first().status == "KO"
        assert RealTimeUpdate.query.first().error == "Http Error"
        assert RealTimeUpdate.query.first().created_at == created_at
        assert RealTimeUpdate.query.first().updated_at > updated_at

        updated_at = RealTimeUpdate.query.first().updated_at

        time.sleep(6)

        manage_db_error("toto", "gtfs-rt", contributor="realtime.sherbrooke", status="KO", error="Http Error")
        assert len(RealTimeUpdate.query.all()) == 1
        assert RealTimeUpdate.query.first().raw_data == "toto"
        assert RealTimeUpdate.query.first().status == "KO"
        assert RealTimeUpdate.query.first().error == "Http Error"
        assert RealTimeUpdate.query.first().created_at == created_at
        assert RealTimeUpdate.query.first().updated_at > updated_at


def test_manage_db_with_http_error_with_insert():
    """
    test the function "manage_db_error" with 'Http Error' since
    no gtfs-rt with 'Http Error' inserted since more than 5 seconds
    """
    with app.app_context():
        manage_db_error("toto", "gtfs-rt", contributor="realtime.sherbrooke", status="KO", error="Http Error")
        assert len(RealTimeUpdate.query.all()) == 1
        assert RealTimeUpdate.query.first().raw_data == "toto"
        assert RealTimeUpdate.query.first().status == "KO"
        assert RealTimeUpdate.query.first().error == "Http Error"

        created_at = RealTimeUpdate.query.first().created_at

        manage_db_error("", "gtfs-rt", contributor="realtime.sherbrooke", status="KO", error="Decode Error")
        assert len(RealTimeUpdate.query.all()) == 2
        assert RealTimeUpdate.query.order_by(desc(RealTimeUpdate.created_at)).first().status == "KO"
        assert RealTimeUpdate.query.order_by(desc(RealTimeUpdate.created_at)).first().error == "Decode Error"
        assert RealTimeUpdate.query.order_by(desc(RealTimeUpdate.created_at)).first().created_at > created_at

        manage_db_error("toto", "gtfs-rt", contributor="realtime.sherbrooke", status="KO", error="Http Error")
        assert len(RealTimeUpdate.query.all()) == 3
        assert RealTimeUpdate.query.order_by(desc(RealTimeUpdate.created_at)).first().raw_data == "toto"
        assert RealTimeUpdate.query.order_by(desc(RealTimeUpdate.created_at)).first().status == "KO"
        assert RealTimeUpdate.query.order_by(desc(RealTimeUpdate.created_at)).first().error == "Http Error"
        assert RealTimeUpdate.query.order_by(desc(RealTimeUpdate.created_at)).first().created_at != created_at


@pytest.fixture()
def pass_midnight_negative_delay_utc_gtfs_rt_data():
    """
    Add tests for pass-midnight UTC on an early passing VJ (negative delay)
    """
    feed = deepcopy(pass_midnight_gtfs_rt_data())
    feed.header.timestamp = to_posix_time(datetime.datetime(year=2012, month=6, day=16, hour=1))
    feed.entity[0].trip_update.trip.trip_id = "Code-pass-midnight-UTC"

    for stu in feed.entity[0].trip_update.stop_time_update:
        stu.arrival.delay = -60
        stu.departure.delay = -60

    return feed


def test_gtfs_pass_midnight_negative_delay_utc_model_builder(pass_midnight_negative_delay_utc_gtfs_rt_data):
    """
    test the model builder with a pass-midnight UTC gtfs-rt
    """
    tester = app.test_client()
    resp = tester.post("/gtfs_rt", data=pass_midnight_negative_delay_utc_gtfs_rt_data.SerializeToString())
    assert resp.status_code == 200

    with app.app_context():
        trip_updates = TripUpdate.query.all()

        assert len(trip_updates) == 1
        assert len(trip_updates[0].stop_time_updates) == 5
        assert RealTimeUpdate.query.first().status == "OK"

        first_stop = trip_updates[0].stop_time_updates[0]
        assert first_stop.stop_id == "StopR1"
        assert first_stop.arrival_status == "update"
        assert first_stop.arrival_delay == timedelta(minutes=-1)
        assert first_stop.departure_status == "update"
        assert first_stop.departure_delay == timedelta(minutes=-1)
        assert first_stop.message is None

        second_stop = trip_updates[0].stop_time_updates[1]
        assert second_stop.stop_id == "StopR2"
        assert second_stop.arrival_status == "update"
        assert second_stop.arrival_delay == timedelta(minutes=-1)
        assert second_stop.departure_status == "update"
        assert second_stop.departure_delay == timedelta(minutes=-1)
        assert second_stop.message is None

        second_stop = trip_updates[0].stop_time_updates[2]
        assert second_stop.stop_id == "StopR2-bis"
        assert second_stop.arrival_status == "update"
        assert second_stop.arrival_delay == timedelta(minutes=-1)
        assert second_stop.departure_status == "update"
        assert second_stop.departure_delay == timedelta(minutes=-1)
        assert second_stop.message is None

        third_stop = trip_updates[0].stop_time_updates[3]
        assert third_stop.stop_id == "StopR3"
        assert third_stop.arrival_status == "update"
        assert third_stop.arrival_delay == timedelta(minutes=-1)
        assert third_stop.departure_status == "update"
        assert third_stop.departure_delay == timedelta(minutes=-1)
        assert third_stop.message is None

        fourth_stop = trip_updates[0].stop_time_updates[4]
        assert fourth_stop.stop_id == "StopR4"
        assert fourth_stop.arrival_status == "update"
        assert fourth_stop.arrival_delay == timedelta(minutes=-1)
        assert fourth_stop.departure_status == "update"
        assert fourth_stop.departure_delay == timedelta(minutes=-1)
        assert fourth_stop.message is None

        feed = convert_to_gtfsrt(trip_updates)
        assert feed.entity[0].trip_update.trip.start_date == "20120615"  # must be UTC start date
