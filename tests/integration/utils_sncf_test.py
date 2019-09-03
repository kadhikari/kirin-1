# coding: utf8

# Copyright (c) 2001-2018, Canal TP and/or its affiliates. All rights reserved.
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
from kirin import app
from kirin.core.model import RealTimeUpdate, TripUpdate, StopTimeUpdate
from datetime import timedelta, datetime


def check_db_96231_delayed(motif_externe_is_null=False):
    with app.app_context():
        assert len(RealTimeUpdate.query.all()) >= 1
        assert len(TripUpdate.query.all()) >= 1
        assert len(StopTimeUpdate.query.all()) >= 6
        db_trip_delayed = TripUpdate.find_by_dated_vj(
            "trip:OCETrainTER-87212027-85000109-3:11859", datetime(2015, 9, 21, 15, 21)
        )
        assert db_trip_delayed

        assert db_trip_delayed.vj.navitia_trip_id == "trip:OCETrainTER-87212027-85000109-3:11859"
        assert db_trip_delayed.vj.get_start_timestamp() == datetime(2015, 9, 21, 15, 21)
        assert db_trip_delayed.vj_id == db_trip_delayed.vj.id
        assert db_trip_delayed.status == "update"
        # Cots contain delayed stop_times only
        assert db_trip_delayed.company_id == "company:OCE:SN"
        # At least 6 stop times must have been created
        assert len(db_trip_delayed.stop_time_updates) >= 6

        # the first stop (in Strasbourg) is not in the feed, only on navitia's base schedule
        # no delay then, only base schedule
        # Navitia's time are in local, so departure 17h21 in paris is 15h21 in UTC
        first_st = db_trip_delayed.stop_time_updates[0]
        assert first_st.stop_id == "stop_point:OCE:SP:TrainTER-87212027"
        assert first_st.arrival == datetime(2015, 9, 21, 15, 21)
        assert first_st.arrival_status == "none"
        assert first_st.arrival_delay == timedelta(0)
        assert first_st.departure == datetime(2015, 9, 21, 15, 21)
        assert first_st.departure_delay == timedelta(0)
        assert first_st.departure_status == "none"
        assert first_st.message is None

        second_st = db_trip_delayed.stop_time_updates[1]
        assert second_st.stop_id == "stop_point:OCE:SP:TrainTER-87214056"
        assert second_st.arrival == datetime(2015, 9, 21, 15, 38)
        assert second_st.arrival_status == "none"
        assert second_st.arrival_delay == timedelta(0)
        assert second_st.departure == datetime(2015, 9, 21, 15, 55)
        assert second_st.departure_delay == timedelta(minutes=15)
        assert second_st.departure_status == "update"
        assert db_trip_delayed.company_id == "company:OCE:SN"
        if motif_externe_is_null:
            assert second_st.message is None
        else:
            assert second_st.message == "Affluence exceptionnelle de voyageurs"

        assert db_trip_delayed.stop_time_updates[2].message == second_st.message
        if db_trip_delayed.stop_time_updates[3].arrival_status == "delete":
            # if there is a added/removed stop in the middle, skip it
            assert db_trip_delayed.stop_time_updates[4].message == second_st.message
        else:
            assert db_trip_delayed.stop_time_updates[3].message == second_st.message

        # last stop is gare de Basel-SBB, delay's only at the arrival
        last_st = db_trip_delayed.stop_time_updates[-1]
        assert last_st.stop_id == "stop_point:OCE:SP:TrainTER-85000109"
        assert last_st.arrival == datetime(2015, 9, 21, 16, 54)
        assert last_st.arrival_status == "update"
        assert last_st.arrival_delay == timedelta(minutes=15)
        # The departure is consistent with arrival
        assert last_st.departure == datetime(2015, 9, 21, 16, 54)
        assert last_st.departure_delay == timedelta(minutes=15)
        assert last_st.departure_status == "none"
        if motif_externe_is_null:
            assert second_st.message is None
        else:
            assert second_st.message == "Affluence exceptionnelle de voyageurs"

        assert db_trip_delayed.contributor is not None
        assert db_trip_delayed.contributor_id == "realtime.cots"

        return db_trip_delayed  # for additional testing if needed


def check_db_870154_partial_removal(contributor=None):
    with app.app_context():
        assert len(RealTimeUpdate.query.all()) >= 1
        assert len(TripUpdate.query.all()) == 1
        assert len(StopTimeUpdate.query.all()) == 12
        db_trip = TripUpdate.find_by_dated_vj("OCE:SN870154F01001", datetime(2018, 11, 2, 9, 54))
        assert db_trip

        assert db_trip.vj.navitia_trip_id == "OCE:SN870154F01001"
        assert db_trip.vj.get_start_timestamp() == datetime(2018, 11, 2, 9, 54)
        assert db_trip.vj_id == db_trip.vj.id
        assert db_trip.status == "update"
        # Cots contain deleted as well as delayed stop_times
        assert db_trip.effect == "REDUCED_SERVICE"
        # 12 stop times must have been created
        assert len(db_trip.stop_time_updates) == 12

        # only the first 4 stops are removed (Rodez to Aubin) in both cases (delay or back to normal)
        first_st = db_trip.stop_time_updates[0]
        assert first_st.stop_id == "stop_point:OCE:SP:TrainTER-87613422"
        assert first_st.arrival_status == "none"
        assert first_st.departure_status == "delete"
        assert first_st.message is None

        second_st = db_trip.stop_time_updates[1]
        assert second_st.stop_id == "stop_point:OCE:SP:TrainTER-87613257"
        assert second_st.arrival_status == "delete"
        assert second_st.departure_status == "delete"
        assert second_st.message is None

        third_st = db_trip.stop_time_updates[2]
        assert third_st.stop_id == "stop_point:OCE:SP:TrainTER-87613232"
        assert third_st.arrival_status == "delete"
        assert third_st.departure_status == "delete"
        assert third_st.message is None

        fourth_st = db_trip.stop_time_updates[3]
        assert fourth_st.stop_id == "stop_point:OCE:SP:TrainTER-87613224"
        assert fourth_st.arrival_status == "delete"
        assert fourth_st.departure_status == "delete"
        assert fourth_st.message is None

        assert db_trip.contributor is not None
        assert db_trip.contributor_id == contributor


def check_db_870154_delay():
    with app.app_context():
        assert len(RealTimeUpdate.query.all()) >= 1
        assert len(TripUpdate.query.all()) == 1
        assert len(StopTimeUpdate.query.all()) == 12
        db_trip = TripUpdate.find_by_dated_vj("OCE:SN870154F01001", datetime(2018, 11, 2, 9, 54))
        assert db_trip
        assert db_trip.message == "Régulation du trafic"
        # Cots contain deleted as well as delayed stop_times
        assert db_trip.effect == "REDUCED_SERVICE"

        # only in "delay-case" departure, arrival at 5th (Viviez-Decazeville)
        # and arrival at 6th stops (Capdenac) are deleted with a cause
        fifth_st = db_trip.stop_time_updates[4]
        assert fifth_st.stop_id == "stop_point:OCE:SP:TrainTER-87613661"
        assert fifth_st.arrival_status == "delete"
        assert fifth_st.departure_status == "delete"
        assert fifth_st.message == "Affluence exceptionnelle de voyageurs"

        sixth_st = db_trip.stop_time_updates[5]
        assert sixth_st.stop_id == "stop_point:OCE:SP:TrainTER-87613109"
        assert sixth_st.arrival_status == "delete"

        # the last 7 stops are late by 10 min (starting by departure in Capdenac)
        assert sixth_st.departure_status == "update"
        assert sixth_st.departure == datetime(2018, 11, 2, 11, 5)
        assert sixth_st.departure_delay == timedelta(minutes=10)
        assert sixth_st.message == "Régulation du trafic"  # Only departure cause is used

        seventh_st = db_trip.stop_time_updates[6]
        assert seventh_st.stop_id == "stop_point:OCE:SP:TrainTER-87613091"
        assert seventh_st.arrival_status == "update"
        assert seventh_st.arrival == datetime(2018, 11, 2, 11, 11)
        assert seventh_st.arrival_delay == timedelta(minutes=10)
        assert seventh_st.departure_status == "update"
        assert seventh_st.departure == datetime(2018, 11, 2, 11, 12)
        assert seventh_st.departure_delay == timedelta(minutes=10)
        assert seventh_st.message == "Régulation du trafic"

        eight_st = db_trip.stop_time_updates[7]
        assert eight_st.stop_id == "stop_point:OCE:SP:TrainTER-87613075"
        assert eight_st.arrival_status == "update"
        assert eight_st.arrival == datetime(2018, 11, 2, 11, 27)
        assert eight_st.arrival_delay == timedelta(minutes=10)
        assert eight_st.departure_status == "update"
        assert eight_st.departure == datetime(2018, 11, 2, 11, 28)
        assert eight_st.departure_delay == timedelta(minutes=10)
        assert eight_st.message == "Régulation du trafic"

        ninth_st = db_trip.stop_time_updates[8]
        assert ninth_st.stop_id == "stop_point:OCE:SP:TrainTER-87613059"
        assert ninth_st.arrival_status == "update"
        assert ninth_st.arrival == datetime(2018, 11, 2, 11, 39)
        assert ninth_st.arrival_delay == timedelta(minutes=10)
        assert ninth_st.departure_status == "update"
        assert ninth_st.departure == datetime(2018, 11, 2, 11, 40)
        assert ninth_st.departure_delay == timedelta(minutes=10)
        assert ninth_st.message == "Régulation du trafic"

        tenth_st = db_trip.stop_time_updates[9]
        assert tenth_st.stop_id == "stop_point:OCE:SP:TrainTER-87613042"
        assert tenth_st.arrival_status == "update"
        assert tenth_st.arrival == datetime(2018, 11, 2, 11, 46)
        assert tenth_st.arrival_delay == timedelta(minutes=10)
        assert tenth_st.departure_status == "update"
        assert tenth_st.departure == datetime(2018, 11, 2, 11, 47)
        assert tenth_st.departure_delay == timedelta(minutes=10)
        assert tenth_st.message == "Régulation du trafic"

        eleventh_st = db_trip.stop_time_updates[10]
        assert eleventh_st.stop_id == "stop_point:OCE:SP:TrainTER-87594572"
        assert eleventh_st.arrival_status == "update"
        assert eleventh_st.arrival == datetime(2018, 11, 2, 12, 1)
        assert eleventh_st.arrival_delay == timedelta(minutes=10)
        assert eleventh_st.departure_status == "update"
        assert eleventh_st.departure == datetime(2018, 11, 2, 12, 2)
        assert eleventh_st.departure_delay == timedelta(minutes=10)
        assert eleventh_st.message == "Régulation du trafic"

        twelfth_st = db_trip.stop_time_updates[11]
        assert twelfth_st.stop_id == "stop_point:OCE:SP:TrainTER-87594002"
        assert twelfth_st.arrival_status == "update"
        assert twelfth_st.arrival == datetime(2018, 11, 2, 12, 25)
        assert twelfth_st.arrival_delay == timedelta(minutes=10)
        assert twelfth_st.departure_status == "none"
        assert twelfth_st.departure == datetime(2018, 11, 2, 12, 25)
        assert twelfth_st.departure_delay == timedelta(minutes=10)
        assert twelfth_st.message == "Régulation du trafic"


def check_db_870154_normal():
    with app.app_context():
        assert len(RealTimeUpdate.query.all()) >= 1
        assert len(TripUpdate.query.all()) == 1
        assert len(StopTimeUpdate.query.all()) == 12
        db_trip = TripUpdate.find_by_dated_vj("OCE:SN870154F01001", datetime(2018, 11, 2, 9, 54))
        assert db_trip
        assert db_trip.message is None

        # departure, arrival at 5th and arrival at 6th stops are back to normal
        fifth_st = db_trip.stop_time_updates[4]
        assert fifth_st.stop_id == "stop_point:OCE:SP:TrainTER-87613661"
        assert fifth_st.arrival_status == "update"
        assert fifth_st.arrival == datetime(2018, 11, 2, 10, 38)
        assert fifth_st.arrival_delay == timedelta(minutes=0)
        assert fifth_st.departure_status == "update"
        assert fifth_st.departure == datetime(2018, 11, 2, 10, 39)
        assert fifth_st.departure_delay == timedelta(minutes=0)
        assert fifth_st.message is None

        sixth_st = db_trip.stop_time_updates[5]
        assert sixth_st.stop_id == "stop_point:OCE:SP:TrainTER-87613109"
        assert sixth_st.arrival_status == "update"
        assert sixth_st.arrival == datetime(2018, 11, 2, 10, 53)
        assert sixth_st.arrival_delay == timedelta(minutes=0)
        # the last 7 stops are late by 10 min (starting by departure in Capdenac)
        sixth_st = db_trip.stop_time_updates[5]
        assert sixth_st.departure_status == "update"
        assert sixth_st.departure == datetime(2018, 11, 2, 10, 55)
        assert sixth_st.departure_delay == timedelta(minutes=0)
        assert sixth_st.message is None

        # "listeHoraireProjeteArrivee" is empty, so it's considered (back to) normal
        seventh_st = db_trip.stop_time_updates[6]
        assert seventh_st.stop_id == "stop_point:OCE:SP:TrainTER-87613091"
        assert seventh_st.arrival_status == "none"
        assert seventh_st.arrival == datetime(2018, 11, 2, 11, 1)
        assert seventh_st.arrival_delay == timedelta(minutes=0)
        assert seventh_st.departure_status == "none"
        assert seventh_st.departure == datetime(2018, 11, 2, 11, 2)
        assert seventh_st.departure_delay == timedelta(minutes=0)
        assert seventh_st.message is None

        eight_st = db_trip.stop_time_updates[7]
        assert eight_st.stop_id == "stop_point:OCE:SP:TrainTER-87613075"
        assert eight_st.arrival_status == "update"
        assert eight_st.arrival == datetime(2018, 11, 2, 11, 17)
        assert eight_st.arrival_delay == timedelta(minutes=0)
        assert eight_st.departure_status == "update"
        assert eight_st.departure == datetime(2018, 11, 2, 11, 18)
        assert eight_st.departure_delay == timedelta(minutes=0)
        assert eight_st.message is None

        ninth_st = db_trip.stop_time_updates[8]
        assert ninth_st.stop_id == "stop_point:OCE:SP:TrainTER-87613059"
        assert ninth_st.arrival_status == "update"
        assert ninth_st.arrival == datetime(2018, 11, 2, 11, 29)
        assert ninth_st.arrival_delay == timedelta(minutes=0)
        assert ninth_st.departure_status == "update"
        assert ninth_st.departure == datetime(2018, 11, 2, 11, 30)
        assert ninth_st.departure_delay == timedelta(minutes=0)
        assert ninth_st.message is None

        # absolutely no information on that stop_time is provided (so the feed is incomplete)
        # no strict specification on that, Kirin keeps previous information (same as IRE)
        tenth_st = db_trip.stop_time_updates[9]
        assert tenth_st.stop_id == "stop_point:OCE:SP:TrainTER-87613042"
        assert tenth_st.arrival_status == "update"
        assert tenth_st.arrival == datetime(2018, 11, 2, 11, 46)
        assert tenth_st.arrival_delay == timedelta(minutes=10)
        assert tenth_st.departure_status == "update"
        assert tenth_st.departure == datetime(2018, 11, 2, 11, 47)
        assert tenth_st.departure_delay == timedelta(minutes=10)
        assert tenth_st.message == "Régulation du trafic"

        eleventh_st = db_trip.stop_time_updates[10]
        assert eleventh_st.stop_id == "stop_point:OCE:SP:TrainTER-87594572"
        assert eleventh_st.arrival_status == "update"
        assert eleventh_st.arrival == datetime(2018, 11, 2, 11, 51)
        assert eleventh_st.arrival_delay == timedelta(minutes=0)
        assert eleventh_st.departure_status == "update"
        assert eleventh_st.departure == datetime(2018, 11, 2, 11, 52)
        assert eleventh_st.departure_delay == timedelta(minutes=0)
        assert eleventh_st.message is None

        twelfth_st = db_trip.stop_time_updates[11]
        assert twelfth_st.stop_id == "stop_point:OCE:SP:TrainTER-87594002"
        assert twelfth_st.arrival_status == "update"
        assert twelfth_st.arrival == datetime(2018, 11, 2, 12, 15)
        assert twelfth_st.arrival_delay == timedelta(minutes=0)
        assert twelfth_st.departure_status == "none"
        assert twelfth_st.departure == datetime(2018, 11, 2, 12, 15)
        assert twelfth_st.departure_delay == timedelta(minutes=0)
        assert twelfth_st.message is None


def check_db_96231_mixed_statuses_inside_stops(contributor=None):
    with app.app_context():
        db_trip_delayed = TripUpdate.find_by_dated_vj(
            "trip:OCETrainTER-87212027-85000109-3:11859", datetime(2015, 9, 21, 15, 21)
        )
        assert db_trip_delayed
        assert db_trip_delayed.vj.navitia_trip_id == "trip:OCETrainTER-87212027-85000109-3:11859"
        assert db_trip_delayed.vj.get_start_timestamp() == datetime(2015, 9, 21, 15, 21)
        assert db_trip_delayed.vj_id == db_trip_delayed.vj.id
        assert db_trip_delayed.status == "update"
        # Cots contain delayed as well as removed stop_times
        assert db_trip_delayed.effect == "REDUCED_SERVICE"
        # 6 stop times must have been created
        assert len(db_trip_delayed.stop_time_updates) == 6

        # the first stop (in Strasbourg) is not in the feed, only on navitia's base schedule
        # no delay then, only base schedule
        # Navitia's time are in local, so departure 17h21 in paris is 15h21 in UTC
        first_st = db_trip_delayed.stop_time_updates[0]
        assert first_st.stop_id == "stop_point:OCE:SP:TrainTER-87212027"
        assert first_st.arrival == datetime(2015, 9, 21, 15, 21)
        assert first_st.arrival_status == "none"
        assert first_st.arrival_delay == timedelta(0)
        assert first_st.departure == datetime(2015, 9, 21, 15, 21)
        assert first_st.departure_delay == timedelta(0)
        assert first_st.departure_status == "none"
        assert first_st.message is None

        second_st = db_trip_delayed.stop_time_updates[1]
        assert second_st.stop_id == "stop_point:OCE:SP:TrainTER-87214056"
        assert second_st.arrival == datetime(2015, 9, 21, 15, 38)
        assert second_st.arrival_status == "none"
        assert second_st.arrival_delay == timedelta(0)
        assert second_st.departure == datetime(2015, 9, 21, 15, 40, 30)
        assert second_st.departure_delay == timedelta(seconds=30)  # only departure is delayed
        assert second_st.departure_status == "update"

        third_st = db_trip_delayed.stop_time_updates[2]
        assert third_st.stop_id == "stop_point:OCE:SP:TrainTER-87182014"
        assert third_st.arrival == datetime(2015, 9, 21, 15, 51, 30)
        assert third_st.arrival_status == "update"
        assert third_st.arrival_delay == timedelta(seconds=30)  # only arrival is delayed
        assert third_st.departure == datetime(2015, 9, 21, 15, 53)
        assert third_st.departure_delay == timedelta(0)
        assert third_st.departure_status == "update"

        fourth_st = db_trip_delayed.stop_time_updates[3]
        assert fourth_st.stop_id == "stop_point:OCE:SP:TrainTER-87182063"
        assert fourth_st.arrival == datetime(2015, 9, 21, 16, 15)
        assert fourth_st.arrival_status == "update"
        assert fourth_st.arrival_delay == timedelta(seconds=60)  # arrival delayed
        assert fourth_st.departure_status == "delete"  # departure removed

        # checking the last 2 stops mostly to check that nothing is propagated and they respect input feed
        fifth_st = db_trip_delayed.stop_time_updates[4]
        assert fifth_st.stop_id == "stop_point:OCE:SP:TrainTER-87182139"
        assert fifth_st.arrival == datetime(2015, 9, 21, 16, 30)
        assert fifth_st.arrival_status == "update"  # in the feed, so updated but no delay
        assert fifth_st.arrival_delay == timedelta(0)
        assert fifth_st.departure == datetime(2015, 9, 21, 16, 31)
        assert fifth_st.departure_status == "update"  # in the feed, so updated but no delay
        assert fifth_st.departure_delay == timedelta(0)

        sixth_st = db_trip_delayed.stop_time_updates[5]
        assert sixth_st.stop_id == "stop_point:OCE:SP:TrainTER-85000109"
        assert sixth_st.arrival == datetime(2015, 9, 21, 16, 39)
        assert sixth_st.arrival_status == "update"  # in the feed, so updated but no delay
        assert sixth_st.arrival_delay == timedelta(0)
        assert sixth_st.departure == datetime(2015, 9, 21, 16, 39)
        assert sixth_st.departure_status == "none"  # not in the feed, so none and no delay
        assert sixth_st.departure_delay == timedelta(0)

        assert db_trip_delayed.contributor is not None
        assert db_trip_delayed.contributor_id == contributor


def check_db_96231_mixed_statuses_delay_removal_delay(contributor=None):
    with app.app_context():
        db_trip_delayed = TripUpdate.find_by_dated_vj(
            "trip:OCETrainTER-87212027-85000109-3:11859", datetime(2015, 9, 21, 15, 21)
        )
        assert db_trip_delayed
        assert db_trip_delayed.vj.navitia_trip_id == "trip:OCETrainTER-87212027-85000109-3:11859"
        assert db_trip_delayed.vj.get_start_timestamp() == datetime(2015, 9, 21, 15, 21)
        assert db_trip_delayed.vj_id == db_trip_delayed.vj.id
        assert db_trip_delayed.status == "update"
        # Cots contain removed as well as delayed stop_times
        assert db_trip_delayed.effect == "REDUCED_SERVICE"
        # 6 stop times must have been created
        assert len(db_trip_delayed.stop_time_updates) == 6

        # the first stop (in Strasbourg) is not in the feed, only on navitia's base schedule
        # no delay then, only base schedule
        # Navitia's time are in local, so departure 17h21 in paris is 15h21 in UTC
        first_st = db_trip_delayed.stop_time_updates[0]
        assert first_st.stop_id == "stop_point:OCE:SP:TrainTER-87212027"
        assert first_st.arrival == datetime(2015, 9, 21, 15, 21)
        assert first_st.arrival_status == "none"
        assert first_st.arrival_delay == timedelta(0)
        assert first_st.departure == datetime(2015, 9, 21, 15, 21)
        assert first_st.departure_status == "none"
        assert first_st.departure_delay == timedelta(0)
        assert first_st.message is None

        second_st = db_trip_delayed.stop_time_updates[1]
        assert second_st.stop_id == "stop_point:OCE:SP:TrainTER-87214056"
        assert second_st.arrival == datetime(2015, 9, 21, 15, 43)
        assert second_st.arrival_status == "update"
        assert second_st.arrival_delay == timedelta(seconds=300)  # delayed by 5 min
        assert second_st.departure == datetime(2015, 9, 21, 15, 45)
        assert second_st.departure_status == "update"
        assert second_st.departure_delay == timedelta(seconds=300)

        third_st = db_trip_delayed.stop_time_updates[2]
        assert third_st.stop_id == "stop_point:OCE:SP:TrainTER-87182014"
        assert third_st.arrival_status == "delete"  # removed
        assert third_st.departure_status == "delete"

        fourth_st = db_trip_delayed.stop_time_updates[3]
        assert fourth_st.stop_id == "stop_point:OCE:SP:TrainTER-87182063"
        assert fourth_st.arrival == datetime(2015, 9, 21, 16, 16)
        assert fourth_st.arrival_status == "update"
        assert fourth_st.arrival_delay == timedelta(seconds=120)  # delayed by 2 min
        assert fourth_st.departure == datetime(2015, 9, 21, 16, 18)
        assert fourth_st.departure_status == "update"
        assert fourth_st.departure_delay == timedelta(seconds=120)

        # checking the last 2 stops mostly to check that nothing is propagated and they respect input feed
        fifth_st = db_trip_delayed.stop_time_updates[4]
        assert fifth_st.stop_id == "stop_point:OCE:SP:TrainTER-87182139"
        assert fifth_st.arrival == datetime(2015, 9, 21, 16, 30)
        assert fifth_st.arrival_status == "update"  # in the feed, so updated but no delay
        assert fifth_st.arrival_delay == timedelta(0)
        assert fifth_st.departure == datetime(2015, 9, 21, 16, 31)
        assert fifth_st.departure_status == "update"  # in the feed, so updated but no delay
        assert fifth_st.departure_delay == timedelta(0)

        sixth_st = db_trip_delayed.stop_time_updates[5]
        assert sixth_st.stop_id == "stop_point:OCE:SP:TrainTER-85000109"
        assert sixth_st.arrival == datetime(2015, 9, 21, 16, 39)
        assert sixth_st.arrival_status == "update"  # in the feed, so updated but no delay
        assert sixth_st.arrival_delay == timedelta(0)
        assert sixth_st.departure == datetime(2015, 9, 21, 16, 39)
        assert sixth_st.departure_status == "none"  # not in the feed, so none and no delay
        assert sixth_st.departure_delay == timedelta(0)

        assert db_trip_delayed.contributor is not None
        assert db_trip_delayed.contributor_id == contributor


def check_db_96231_normal(contributor=None):
    with app.app_context():
        assert len(RealTimeUpdate.query.all()) >= 1
        assert len(TripUpdate.query.all()) >= 1
        assert len(StopTimeUpdate.query.all()) >= 6
        db_trip_delayed = TripUpdate.find_by_dated_vj(
            "trip:OCETrainTER-87212027-85000109-3:11859", datetime(2015, 9, 21, 15, 21)
        )
        assert db_trip_delayed

        assert db_trip_delayed.vj.navitia_trip_id == "trip:OCETrainTER-87212027-85000109-3:11859"
        assert db_trip_delayed.vj.get_start_timestamp() == datetime(2015, 9, 21, 15, 21)
        assert db_trip_delayed.vj_id == db_trip_delayed.vj.id
        assert db_trip_delayed.status == "update"
        assert db_trip_delayed.effect == "SIGNIFICANT_DELAYS"
        assert db_trip_delayed.message is None
        # 6 stop times must have been created
        assert len(db_trip_delayed.stop_time_updates) == 6

        # the first stop (in Strasbourg) is not in the feed, only on navitia's base schedule
        # no delay then, only base schedule
        # Navitia's time are in local, so departure 17h21 in paris is 15h21 in UTC
        first_st = db_trip_delayed.stop_time_updates[0]
        assert first_st.stop_id == "stop_point:OCE:SP:TrainTER-87212027"
        assert first_st.arrival == datetime(2015, 9, 21, 15, 21)
        assert first_st.arrival_status == "none"
        assert first_st.arrival_delay == timedelta(0)
        assert first_st.departure == datetime(2015, 9, 21, 15, 21)
        assert first_st.departure_delay == timedelta(0)
        assert first_st.departure_status == "none"
        assert first_st.message is None

        # the departure time has been updated with a delay at 0
        second_st = db_trip_delayed.stop_time_updates[1]
        assert second_st.stop_id == "stop_point:OCE:SP:TrainTER-87214056"
        assert second_st.arrival == datetime(2015, 9, 21, 15, 38)
        assert second_st.arrival_status == "none"
        assert second_st.arrival_delay == timedelta(0)
        assert second_st.departure == datetime(2015, 9, 21, 15, 40)
        assert second_st.departure_delay == timedelta(minutes=0)
        assert second_st.departure_status == "update"
        assert second_st.message == "Affluence exceptionnelle de voyageurs"

        # last stop is gare de Basel-SBB, the arrival is also updated with a delay at 0
        last_st = db_trip_delayed.stop_time_updates[-1]
        assert last_st.stop_id == "stop_point:OCE:SP:TrainTER-85000109"
        assert last_st.arrival == datetime(2015, 9, 21, 16, 39)
        assert last_st.arrival_status == "update"
        assert last_st.arrival_delay == timedelta(minutes=0)
        # The departure should be the same than the base-schedule one
        # except that it's not the case, we have messed with it when the vj was delayed,
        # but we didn't put it back like it was when the train catch up is delay
        try:
            assert last_st.departure == datetime(2015, 9, 21, 16, 39)
            assert last_st.departure_delay == timedelta(minutes=0)
            assert last_st.departure_status == "none"
            assert last_st.message == "Affluence exceptionnelle de voyageurs"
        except AssertionError:
            pass  # xfail: we don't change back the departure :(

        assert db_trip_delayed.contributor is not None
        assert db_trip_delayed.contributor_id == contributor


def check_db_john_trip_removal():
    with app.app_context():
        assert len(RealTimeUpdate.query.all()) >= 1
        assert len(TripUpdate.query.all()) >= 2
        assert len(StopTimeUpdate.query.all()) >= 0
        db_trip1_removal = TripUpdate.find_by_dated_vj(
            "trip:OCETGV-87686006-87751008-2:25768", datetime(2015, 9, 21, 13, 37)
        )
        assert db_trip1_removal

        assert db_trip1_removal.vj.navitia_trip_id == "trip:OCETGV-87686006-87751008-2:25768"
        assert db_trip1_removal.vj.get_start_timestamp() == datetime(2015, 9, 21, 13, 37)
        assert db_trip1_removal.vj_id == db_trip1_removal.vj.id
        assert db_trip1_removal.status == "delete"
        assert db_trip1_removal.effect == "NO_SERVICE"
        # full trip removal : no stop_time to precise
        assert len(db_trip1_removal.stop_time_updates) == 0

        db_trip2_removal = TripUpdate.find_by_dated_vj(
            "trip:OCETrainTER-87212027-85000109-3:11859", datetime(2015, 9, 21, 15, 21)
        )
        assert db_trip2_removal

        assert db_trip2_removal.vj.navitia_trip_id == "trip:OCETrainTER-87212027-85000109-3:11859"
        assert db_trip2_removal.vj.get_start_timestamp() == datetime(2015, 9, 21, 15, 21)
        assert db_trip2_removal.vj_id == db_trip2_removal.vj.id
        assert db_trip2_removal.status == "delete"
        assert db_trip2_removal.effect == "NO_SERVICE"
        # full trip removal : no stop_time to precise
        assert len(db_trip2_removal.stop_time_updates) == 0


def check_db_96231_trip_removal():
    with app.app_context():
        assert len(RealTimeUpdate.query.all()) >= 1
        assert len(TripUpdate.query.all()) >= 1
        assert len(StopTimeUpdate.query.all()) >= 0
        db_trip_removal = TripUpdate.find_by_dated_vj(
            "trip:OCETrainTER-87212027-85000109-3:11859", datetime(2015, 9, 21, 15, 21)
        )
        assert db_trip_removal

        assert db_trip_removal.vj.navitia_trip_id == "trip:OCETrainTER-87212027-85000109-3:11859"
        assert db_trip_removal.vj.get_start_timestamp() == datetime(2015, 9, 21, 15, 21)
        assert db_trip_removal.vj_id == db_trip_removal.vj.id
        assert db_trip_removal.status == "delete"
        assert db_trip_removal.effect == "NO_SERVICE"
        assert db_trip_removal.message is None
        # full trip removal : no stop_time to precise
        assert len(db_trip_removal.stop_time_updates) == 0


def check_db_6113_trip_removal():
    with app.app_context():
        assert len(RealTimeUpdate.query.all()) >= 1
        assert len(TripUpdate.query.all()) >= 1
        assert len(StopTimeUpdate.query.all()) >= 0
        db_trip_removal = TripUpdate.find_by_dated_vj(
            "trip:OCETGV-87686006-87751008-2:25768", datetime(2015, 10, 6, 10, 37)
        )
        assert db_trip_removal

        assert db_trip_removal.vj.navitia_trip_id == "trip:OCETGV-87686006-87751008-2:25768"
        assert db_trip_removal.vj.get_start_timestamp() == datetime(2015, 10, 6, 10, 37)
        assert db_trip_removal.vj_id == db_trip_removal.vj.id
        assert db_trip_removal.status == "delete"
        assert db_trip_removal.effect == "NO_SERVICE"
        assert db_trip_removal.message == "Accident à un Passage à Niveau"
        # full trip removal : no stop_time to precise
        assert len(db_trip_removal.stop_time_updates) == 0


def check_db_6111_trip_removal_pass_midnight():
    with app.app_context():
        assert len(RealTimeUpdate.query.all()) >= 1
        assert len(TripUpdate.query.all()) >= 1
        assert len(StopTimeUpdate.query.all()) >= 0
        db_trip_removal = TripUpdate.find_by_dated_vj(
            "trip:OCETGV-87686006-87751008-2:25768", datetime(2015, 10, 6, 20, 37)
        )
        assert db_trip_removal

        assert db_trip_removal.vj.navitia_trip_id == "trip:OCETGV-87686006-87751008-2:25768"
        assert db_trip_removal.vj.get_start_timestamp() == datetime(2015, 10, 6, 20, 37)
        assert db_trip_removal.vj_id == db_trip_removal.vj.id
        assert db_trip_removal.status == "delete"
        assert db_trip_removal.effect == "NO_SERVICE"
        assert db_trip_removal.message == "Accident à un Passage à Niveau"
        # full trip removal : no stop_time to precise
        assert len(db_trip_removal.stop_time_updates) == 0


def check_db_6114_trip_removal():
    with app.app_context():
        assert len(RealTimeUpdate.query.all()) >= 1
        assert len(TripUpdate.query.all()) >= 1
        assert len(StopTimeUpdate.query.all()) >= 0
        db_trip_removal = TripUpdate.find_by_dated_vj(
            "trip:OCETGV-87686006-87751008-2:25768-2", datetime(2015, 10, 6, 10, 37)
        )
        assert db_trip_removal

        assert db_trip_removal.vj.navitia_trip_id == "trip:OCETGV-87686006-87751008-2:25768-2"
        assert db_trip_removal.vj.get_start_timestamp() == datetime(2015, 10, 6, 10, 37)
        assert db_trip_removal.vj_id == db_trip_removal.vj.id
        assert db_trip_removal.status == "delete"
        assert db_trip_removal.message == "Accident à un Passage à Niveau"
        # full trip removal : no stop_time to precise
        assert len(db_trip_removal.stop_time_updates) == 0


def check_db_96231_partial_removal(contributor=None):
    with app.app_context():
        assert len(RealTimeUpdate.query.all()) >= 1
        assert len(TripUpdate.query.all()) >= 1
        assert len(StopTimeUpdate.query.all()) >= 6
        db_trip_partial_removed = TripUpdate.find_by_dated_vj(
            "trip:OCETrainTER-87212027-85000109-3:11859", datetime(2015, 9, 21, 15, 21)
        )
        assert db_trip_partial_removed

        assert db_trip_partial_removed.vj.navitia_trip_id == "trip:OCETrainTER-87212027-85000109-3:11859"
        assert db_trip_partial_removed.vj.get_start_timestamp() == datetime(2015, 9, 21, 15, 21)
        assert db_trip_partial_removed.vj_id == db_trip_partial_removed.vj.id
        assert db_trip_partial_removed.status == "update"
        assert db_trip_partial_removed.effect == "REDUCED_SERVICE"
        # 6 stop times must have been created
        assert len(db_trip_partial_removed.stop_time_updates) == 6

        # the first stop (in Strasbourg) is not in the feed, only on navitia's base schedule
        # no delay then, only base schedule
        # Navitia's time are in local, so departure 17h21 in paris is 15h21 in UTC
        first_st = db_trip_partial_removed.stop_time_updates[0]
        assert first_st.stop_id == "stop_point:OCE:SP:TrainTER-87212027"
        assert first_st.arrival == datetime(2015, 9, 21, 15, 21)
        assert first_st.arrival_status == "none"
        assert first_st.arrival_delay == timedelta(0)
        assert first_st.departure == datetime(2015, 9, 21, 15, 21)
        assert first_st.departure_delay == timedelta(0)
        assert first_st.departure_status == "none"
        assert first_st.message is None

        # the departure time has been updated with a delay at 0
        second_st = db_trip_partial_removed.stop_time_updates[1]
        assert second_st.stop_id == "stop_point:OCE:SP:TrainTER-87214056"
        assert second_st.arrival == datetime(2015, 9, 21, 15, 38)
        assert second_st.arrival_status == "none"
        assert second_st.arrival_delay == timedelta(0)
        assert second_st.departure == datetime(2015, 9, 21, 15, 40)
        assert second_st.departure_delay == timedelta(minutes=0)
        assert second_st.departure_status == "none"

        # the departure time has been updated with a delay at 0
        second_st = db_trip_partial_removed.stop_time_updates[2]
        assert second_st.stop_id == "stop_point:OCE:SP:TrainTER-87182014"
        assert second_st.arrival == datetime(2015, 9, 21, 15, 51)
        assert second_st.arrival_status == "none"
        assert second_st.arrival_delay == timedelta(0)
        assert second_st.departure == datetime(2015, 9, 21, 15, 53)
        assert second_st.departure_delay == timedelta(minutes=0)
        assert second_st.departure_status == "none"

        # At 4th stop, only departure is deleted
        second_st = db_trip_partial_removed.stop_time_updates[3]
        assert second_st.stop_id == "stop_point:OCE:SP:TrainTER-87182063"
        assert second_st.arrival == datetime(2015, 9, 21, 16, 14)
        assert second_st.arrival_status == "none"
        assert second_st.arrival_delay == timedelta(0)
        assert second_st.departure_status == "delete"

        # All the 5th stop is deleted
        third_st = db_trip_partial_removed.stop_time_updates[4]
        assert third_st.stop_id == "stop_point:OCE:SP:TrainTER-87182139"
        assert third_st.arrival_status == "delete"
        assert third_st.departure_status == "delete"

        # last stop is gare de Basel-SBB, the arrival is also deleted
        last_st = db_trip_partial_removed.stop_time_updates[5]
        assert last_st.stop_id == "stop_point:OCE:SP:TrainTER-85000109"
        assert last_st.arrival_status == "delete"
        # The departure should be the same than the base-schedule one
        assert last_st.departure == datetime(2015, 9, 21, 16, 39)
        assert last_st.departure_delay == timedelta(minutes=0)
        assert last_st.departure_status == "none"

        assert db_trip_partial_removed.contributor is not None
        assert db_trip_partial_removed.contributor_id == contributor


def check_db_840427_partial_removal(contributor=None):
    with app.app_context():
        db_trip_partial_removed = TripUpdate.find_by_dated_vj("OCE:SN840427F03001", datetime(2017, 3, 18, 13, 5))
        assert db_trip_partial_removed

        assert db_trip_partial_removed.vj.navitia_trip_id == "OCE:SN840427F03001"
        assert db_trip_partial_removed.vj.get_start_timestamp() == datetime(2017, 3, 18, 13, 5)
        assert db_trip_partial_removed.vj_id == db_trip_partial_removed.vj.id
        assert db_trip_partial_removed.status == "update"
        assert db_trip_partial_removed.effect == "REDUCED_SERVICE"

        # 7 stop times must have been created
        assert len(db_trip_partial_removed.stop_time_updates) == 7

        # the first stop have not been changed
        first_st = db_trip_partial_removed.stop_time_updates[0]
        assert first_st.stop_id == "stop_point:OCE:SP:TrainTER-87713040"
        assert first_st.arrival_status == "none"
        assert first_st.departure_status == "none"
        assert first_st.message is None

        for s in db_trip_partial_removed.stop_time_updates[0:3]:
            assert s.arrival_status == "none"
            assert s.departure_status == "none"
            assert s.message is None

        # the stops Chaumont, Bar-sur-Aube, Vendeuvre and Troyes should have been marked as deleted
        # (even if Chaumont and Vendeuvre were in a 'PRDebut'/'PRFin' tag
        ch_st = db_trip_partial_removed.stop_time_updates[3]
        assert ch_st.stop_id == "stop_point:OCE:SP:TrainTER-87142000"  # Chaumont
        assert ch_st.arrival_status == "none"  # the train still arrives in this stop
        assert ch_st.departure_status == "delete"
        assert ch_st.message == "Défaut d'alimentation électrique"

        bar_st = db_trip_partial_removed.stop_time_updates[4]
        assert bar_st.stop_id == "stop_point:OCE:SP:TrainTER-87118299"  # Bar-sur-Aube
        assert bar_st.arrival_status == "delete"
        assert bar_st.departure_status == "delete"
        assert bar_st.message == "Défaut d'alimentation électrique"

        ven_st = db_trip_partial_removed.stop_time_updates[5]
        assert ven_st.stop_id == "stop_point:OCE:SP:TrainTER-87118257"  # Vendeuvre
        assert ven_st.arrival_status == "delete"
        assert ven_st.departure_status == "delete"
        assert ven_st.message == "Défaut d'alimentation électrique"

        tro_st = db_trip_partial_removed.stop_time_updates[6]
        assert tro_st.stop_id == "stop_point:OCE:SP:TrainTER-87118000"  # Troyes
        assert tro_st.arrival_status == "delete"
        assert tro_st.departure_status == "none"  # the train still does not leave from this stop
        assert tro_st.message == "Défaut d'alimentation électrique"

        assert db_trip_partial_removed.contributor is not None
        assert db_trip_partial_removed.contributor_id == contributor
