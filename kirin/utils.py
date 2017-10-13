# Copyright (c) 2001-2015, Canal TP and/or its affiliates. All rights reserved.
#
# This file is part of Navitia,
#     the software to build cool stuff with public transport.
#
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
import logging

import pytz
from aniso8601 import parse_date
from pythonjsonlogger import jsonlogger
from flask.globals import current_app
import navitia_wrapper
from kirin import new_relic, redis
from contextlib import contextmanager
from redis.exceptions import ConnectionError


from kirin.core import model


TASK_STOP_MAX_DELAY = current_app.config['TASK_STOP_MAX_DELAY']
TASK_WAIT_FIXED = current_app.config['TASK_WAIT_FIXED']


def make_kirin_lock_name(*args):
    return '|'.join([current_app.config['TASK_LOCK_PREFIX']] + [str(a) for a in args])

@contextmanager
def get_lock(logger, lock_name, lock_timeout):
    logger.debug('getting lock %s', lock_name)
    try:
        lock = redis.lock(lock_name, timeout=lock_timeout)
        locked = lock.acquire(blocking=False)
    except ConnectionError:
        logging.exception('Exception with redis while locking')
        raise

    yield locked

    if locked:
        logger.debug("releasing lock %s", lock_name)
        lock.release()


def should_retry_exception(exception):
    return isinstance(exception, ConnectionError)


def str_to_date(value):
    if not value:
        return None
    try:
        return parse_date(value)
    except:
        logging.getLogger(__name__).info('[{value} invalid date.'.format(value=value))
        return None


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """
    jsonformatter with extra params

    you can add additional params to it (like the environment name) at configuration time
    """
    def __init__(self, *args, **kwargs):
        self.extras = kwargs.pop('extras', {})
        jsonlogger.JsonFormatter.__init__(self, *args, **kwargs)

    def process_log_record(self, log_record):
        log_record.update(self.extras)
        return log_record


def make_navitia_wrapper():
    """
    return a navitia wrapper to call the navitia API
    """
    url = current_app.config['NAVITIA_URL']
    token = current_app.config.get('NAVITIA_TOKEN')
    instance = current_app.config['NAVITIA_INSTANCE']
    return navitia_wrapper.Navitia(url=url, token=token).instance(instance)


def make_rt_update(data, connector):
    """
    Create an RealTimeUpdate object for the query and persist it
    """
    rt_update = model.RealTimeUpdate(data, connector=connector)

    model.db.session.add(rt_update)
    model.db.session.commit()
    return rt_update


def record_internal_failure(message, **kwargs):
    params = {'message': message}
    params.update(kwargs)
    new_relic.record_custom_event('kirin_internal_failure', params)


def record_call(status, **kwargs):
    """
    status can be in: ok, a message text or failure with reason.
    parameters: contributor, timestamp, trip_update_count, size...
    """
    params = {'status': status}
    params.update(kwargs)
    new_relic.record_custom_event('kirin_status', params)


def get_timezone(stop_time):
    # TODO: we must use the coverage timezone, not the stop_area timezone, as they can be different.
    # We don't have this information now but we should have it in the near future
    str_tz = stop_time.get('stop_point', {}).get('stop_area', {}).get('timezone')
    if not str_tz:
        raise Exception('impossible to convert local to utc without the timezone')

    tz = pytz.timezone(str_tz)
    if not tz:
        raise Exception("impossible to find timezone: '{}'".format(str_tz))
    return tz

