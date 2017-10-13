# Copyright (c) 2001-2014, Canal TP and/or its affiliates. All rights reserved.
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

import logging
from kirin.tasks import celery
import datetime
from kirin.core.model import TripUpdate, RealTimeUpdate
from retrying import retry
from kirin import app
from kirin.utils import TASK_STOP_MAX_DELAY, TASK_WAIT_FIXED, make_kirin_lock_name, get_lock, should_retry_exception


@celery.task(bind=True)
@retry(stop_max_delay=TASK_STOP_MAX_DELAY,
       wait_fixed=TASK_WAIT_FIXED,
       retry_on_exception=should_retry_exception)
def ire_purge_trip_update(self, config):
    func_name = 'ire_purge_trip_update'
    contributor = config['contributor']
    logger = logging.LoggerAdapter(logging.getLogger(__name__), extra={'contributor': contributor})
    logger.debug('purge ire trip update for %s', contributor)

    lock_name = make_kirin_lock_name(func_name, contributor)
    with get_lock(logger, lock_name, app.config['REDIS_LOCK_TIMEOUT_PURGE']) as locked:
        if not locked:
            logger.warning('%s for %s is already in progress', func_name, contributor)
            return
        until = datetime.date.today() - datetime.timedelta(days=int(config['nb_days_to_keep']))
        logger.info('purge ire trip update until %s', until)

        TripUpdate.remove_by_contributors_and_period(contributors=[contributor], start_date=None, end_date=until)
        logger.info('%s for %s is finished', func_name, contributor)


@celery.task(bind=True)
@retry(stop_max_delay=TASK_STOP_MAX_DELAY,
       wait_fixed=TASK_WAIT_FIXED,
       retry_on_exception=should_retry_exception)
def ire_purger_rt_update(self, config):
    func_name = 'ire_purger_rt_update'
    connector = config['connector']

    logger = logging.LoggerAdapter(logging.getLogger(__name__), extra={'connector': connector})
    logger.debug('purge ire realtime update for %s', connector)

    lock_name = make_kirin_lock_name(func_name, connector)
    with get_lock(logger, lock_name, app.config['REDIS_LOCK_TIMEOUT_PURGE']) as locked:
        if not locked:
            logger.warning('%s for %s is already in progress', func_name, connector)
            return

        until = datetime.date.today() - datetime.timedelta(days=int(config['nb_days_to_keep']))
        logger.info('purge ire realtime update until %s', until)

        RealTimeUpdate.remove_by_connectors_until(connectors=[connector], until=until)
        logger.info('%s for %s is finished', func_name, connector)
