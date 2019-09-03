# coding=utf-8

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

from __future__ import absolute_import, print_function, unicode_literals, division
import logging
from kirin.core import model


from celery.signals import task_postrun, setup_logging
from kirin.helper import make_celery

from retrying import retry
from kirin import app
import datetime
from kirin.core.model import TripUpdate, RealTimeUpdate
from kirin.utils import should_retry_exception, make_kirin_lock_name, get_lock

TASK_STOP_MAX_DELAY = app.config[str("TASK_STOP_MAX_DELAY")]
TASK_WAIT_FIXED = app.config[str("TASK_WAIT_FIXED")]


# we don't want celery to mess with our logging configuration
@setup_logging.connect
def celery_setup_logging(*args, **kwargs):
    pass


celery = make_celery(app)


@task_postrun.connect
def close_session(*args, **kwargs):
    # Flask SQLAlchemy will automatically create new sessions for you from
    # a scoped session factory, given that we are maintaining the same app
    # context, this ensures tasks have a fresh session (e.g. session errors
    # won't propagate across tasks)
    model.db.session.remove()


@celery.task(bind=True)
@retry(stop_max_delay=TASK_STOP_MAX_DELAY, wait_fixed=TASK_WAIT_FIXED, retry_on_exception=should_retry_exception)
def purge_trip_update(self, config):
    func_name = "purge_trip_update"
    contributor = config["contributor"]
    logger = logging.LoggerAdapter(logging.getLogger(__name__), extra={"contributor": contributor})
    logger.debug("purge trip update for %s", contributor)

    lock_name = make_kirin_lock_name(func_name, contributor)
    with get_lock(logger, lock_name, app.config[str("REDIS_LOCK_TIMEOUT_PURGE")]) as locked:
        if not locked:
            logger.warning("%s for %s is already in progress", func_name, contributor)
            return
        until = datetime.date.today() - datetime.timedelta(days=int(config["nb_days_to_keep"]))
        logger.info("purge trip update for {} until {}".format(contributor, until))

        TripUpdate.remove_by_contributors_and_period(contributors=[contributor], start_date=None, end_date=until)
        logger.info("%s for %s is finished", func_name, contributor)


@celery.task(bind=True)
@retry(stop_max_delay=TASK_STOP_MAX_DELAY, wait_fixed=TASK_WAIT_FIXED, retry_on_exception=should_retry_exception)
def purge_rt_update(self, config):
    func_name = "purge_rt_update"
    connector = config["connector"]

    logger = logging.LoggerAdapter(logging.getLogger(__name__), extra={"connector": connector})
    logger.debug("purge realtime update for %s", connector)

    lock_name = make_kirin_lock_name(func_name, connector)
    with get_lock(logger, lock_name, app.config[str("REDIS_LOCK_TIMEOUT_PURGE")]) as locked:
        if not locked:
            logger.warning("%s for %s is already in progress", func_name, connector)
            return

        until = datetime.date.today() - datetime.timedelta(days=int(config["nb_days_to_keep"]))
        logger.info("purge realtime update for {} until {}".format(connector, until))

        # TODO:  we want to purge on "contributor" later, not "connector".
        RealTimeUpdate.remove_by_connectors_until(connectors=[connector], until=until)
        logger.info("%s for %s is finished", func_name, connector)


from kirin.gtfs_rt.tasks import gtfs_poller


@celery.task(bind=True)
def poller(self):
    # TODO:
    #  For the future multi-gtfs_rt with configurations in the table contributor
    #  parameters values should be updated by parameters of config file if present.
    config = {
        "contributor": app.config.get(str("GTFS_RT_CONTRIBUTOR")),
        "navitia_url": app.config.get(str("NAVITIA_URL")),
        "token": app.config.get(str("NAVITIA_GTFS_RT_TOKEN")),
        "coverage": app.config.get(str("NAVITIA_GTFS_RT_INSTANCE")),
        "feed_url": app.config.get(str("GTFS_RT_FEED_URL")),
    }
    gtfs_poller.delay(config)


@celery.task(bind=True)
def purge_gtfs_trip_update(self):
    """
    This task will remove ONLY TripUpdate, StoptimeUpdate and VehicleJourney that are created by gtfs-rt but the
    RealTimeUpdate are kept so that we can replay it for debug purpose. RealTimeUpdate will be remove by another task
    """
    config = {
        "contributor": app.config.get(str("GTFS_RT_CONTRIBUTOR")),
        "nb_days_to_keep": app.config.get(str("NB_DAYS_TO_KEEP_TRIP_UPDATE")),
    }
    purge_trip_update.delay(config)


@celery.task(bind=True)
def purge_gtfs_rt_update(self):
    """
    This task will remove realtime update
    """
    config = {"nb_days_to_keep": app.config.get(str("NB_DAYS_TO_KEEP_RT_UPDATE")), "connector": "gtfs-rt"}
    purge_rt_update.delay(config)


@celery.task(bind=True)
def purge_cots_trip_update(self):
    """
    This task will remove ONLY TripUpdate, StopTimeUpdate and VehicleJourney that are created by COTS but the
    RealTimeUpdate are kept so that we can replay it for debug purpose. RealTimeUpdate will be remove by another task
    """
    config = {"contributor": app.config.get(str("COTS_CONTRIBUTOR")), "nb_days_to_keep": 10}
    purge_trip_update.delay(config)


@celery.task(bind=True)
def purge_cots_rt_update(self):
    """
    This task will remove realtime update
    """
    config = {"nb_days_to_keep": 100, "connector": "cots"}
    purge_rt_update.delay(config)
