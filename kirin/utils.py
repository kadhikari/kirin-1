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
import logging

import six
from aniso8601 import parse_date
from pythonjsonlogger import jsonlogger
from flask.globals import current_app
import navitia_wrapper
from pytz import utc

from kirin import new_relic
from redis.exceptions import ConnectionError
from contextlib import contextmanager
from kirin.core import model
from kirin.exceptions import InternalException


def floor_datetime(datetime):
    return datetime.replace(minute=0, second=0, microsecond=0)


def str_to_date(value):
    if not value:
        return None
    try:
        return parse_date(value)
    except:
        logging.getLogger(__name__).info("[{value} invalid date.".format(value=value))
        return None


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """
    jsonformatter with extra params

    you can add additional params to it (like the environment name) at configuration time
    """

    def __init__(self, *args, **kwargs):
        self.extras = kwargs.pop("extras", {})
        jsonlogger.JsonFormatter.__init__(self, *args, **kwargs)

    def process_log_record(self, log_record):
        log_record.update(self.extras)
        return log_record


def make_navitia_wrapper():
    """
    return a navitia wrapper to call the navitia API
    """
    # TODO:
    #  For the future multi-gtfs_rt with configurations in the table contributor
    #  parameters values should be updated by parameters of config file if present.
    url = current_app.config[str("NAVITIA_URL")]
    token = current_app.config.get(str("NAVITIA_TOKEN"))
    instance = current_app.config[str("NAVITIA_INSTANCE")]
    return navitia_wrapper.Navitia(url=url, token=token).instance(instance)


def to_navitia_utc_str(naive_utc_dt):
    """
    format a naive UTC datetime to a navitia-readable UTC-aware str
    (to avoid managing coverage's timezone,
    as Navitia considers datetime without timezone as local to the coverage)
    """
    if naive_utc_dt.tzinfo is not None:
        raise InternalException("Invalid datetime provided: must be naive (and UTC)")
    return utc.localize(naive_utc_dt).strftime("%Y%m%dT%H%M%S%z")


def make_rt_update(data, connector, contributor, status="OK"):
    """
    Create an RealTimeUpdate object for the query and persist it
    """
    rt_update = model.RealTimeUpdate(data, connector=connector, contributor=contributor, status=status)

    model.db.session.add(rt_update)
    model.db.session.commit()
    return rt_update


def record_internal_failure(log, **kwargs):
    params = {"log": log}
    params.update(kwargs)
    new_relic.record_custom_event("kirin_internal_failure", params)


def record_call(status, **kwargs):
    """
    status can be in: ok, a message text or failure with reason.
    parameters: contributor, timestamp, trip_update_count, size...
    """
    params = {"status": status}
    params.update(kwargs)
    new_relic.record_custom_event("kirin_status", params)


def should_retry_exception(exception):
    return isinstance(exception, ConnectionError)


def make_kirin_lock_name(*args):
    from kirin import app

    return "|".join([app.config[str("TASK_LOCK_PREFIX")]] + [a for a in args])


def save_gtfs_rt_with_error(data, connector, contributor, status, error=None):
    raw_data = six.binary_type(data)
    rt_update = make_rt_update(raw_data, connector=connector, contributor=contributor, status=status)
    rt_update.status = status
    rt_update.error = error
    model.db.session.add(rt_update)
    model.db.session.commit()


def poke_updated_at(rtu):
    """
    just update the updated_at of the RealTimeUpdate object provided
    """
    if rtu:
        status = rtu.status
        rtu.status = "pending" if status != "pending" else "OK"  # just to poke updated_at
        model.db.session.commit()
        rtu.status = status
        model.db.session.commit()


def manage_db_error(data, connector, contributor, status, error=None):
    """
    If the last RTUpdate contains the same error (and data, status) we just change updated_at:
    This way, we know we had this error between created_at and updated_at, but we don't get extra rows in db

    Otherwise, we create a new one, as we want to track error changes

    parameters: data, connector, contributor, status, error
    """
    last = model.RealTimeUpdate.get_last_rtu(connector, contributor)
    if last and last.status == status and last.error == error and last.raw_data == six.binary_type(data):
        poke_updated_at(last)
    else:
        save_gtfs_rt_with_error(data, connector, contributor, status, error)


def manage_db_no_new(connector, contributor):
    last = model.RealTimeUpdate.get_last_rtu(connector, contributor)
    poke_updated_at(last)


@contextmanager
def get_lock(logger, lock_name, lock_timeout):
    from kirin import redis

    logger.debug("getting lock %s", lock_name)
    try:
        lock = redis.lock(lock_name, timeout=lock_timeout)
        locked = lock.acquire(blocking=False)
    except ConnectionError:
        logging.exception("Exception with redis while locking")
        raise

    try:
        yield locked
    finally:
        if locked:
            logger.debug("releasing lock %s", lock_name)
            lock.release()
