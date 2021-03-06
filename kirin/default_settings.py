# encoding: utf-8

import os
from flask_restful.inputs import boolean
import json
from datetime import timedelta
from celery import schedules

#URI for postgresql
# postgresql://<user>:<password>@<host>:<port>/<dbname>
#http://docs.sqlalchemy.org/en/rel_0_9/dialects/postgresql.html#psycopg2
SQLALCHEMY_DATABASE_URI = os.getenv('KIRIN_SQLALCHEMY_DATABASE_URI', 'postgresql://navitia:navitia@localhost/kirin')

NAVITIA_URL = os.getenv('KIRIN_NAVITIA_URL', None)

NAVITIA_TIMEOUT = 5

NAVITIA_INSTANCE = os.getenv('KIRIN_NAVITIA_INSTANCE', 'sncf')

NAVITIA_TOKEN = os.getenv('KIRIN_NAVITIA_TOKEN', None)

CONTRIBUTOR = os.getenv('KIRIN_CONTRIBUTOR', 'realtime.ire')

CELERY_BROKER_URL = os.getenv('KIRIN_CELERY_BROKER_URL', 'pyamqp://guest:guest@localhost:5672//?heartbeat=60')


# TODO better conf for multi GTFS-RT
NAVITIA_GTFS_RT_INSTANCE = os.getenv('KIRIN_NAVITIA_GTFS_RT_INSTANCE', 'sherbrooke')
NAVITIA_GTFS_RT_TOKEN = os.getenv('KIRIN_NAVITIA_GTFS_RT_TOKEN', None)
GTFS_RT_CONTRIBUTOR = os.getenv('KIRIN_GTFS_RT_CONTRIBUTOR', 'realtime.sherbrooke')
GTFS_RT_FEED_URL = os.getenv('KIRIN_GTFS_RT_FEED_URL', None)


DEBUG = boolean(os.getenv('KIRIN_DEBUG', False))

#rabbitmq connections string: http://kombu.readthedocs.org/en/latest/userguide/connections.html#urls
RABBITMQ_CONNECTION_STRING = os.getenv('KIRIN_RABBITMQ_CONNECTION_STRING', 'pyamqp://guest:guest@localhost:5672//?heartbeat=60')

#time before trying to reconnect to rabbitmq
RETRY_TIMEOUT = 10

#queue used for task of type load_realtime, all instances of kirin must use the same queue
#to be able to load balance tasks between them
LOAD_REALTIME_QUEUE = 'kirin_load_realtime'

#amqp exhange used for sending disruptions
EXCHANGE = os.getenv('KIRIN_RABBITMQ_EXCHANGE', 'navitia')

ENABLE_RABBITMQ = boolean(os.getenv('KIRIN_ENABLE_RABBITMQ', True))

log_level = os.getenv('KIRIN_LOG_LEVEL', 'DEBUG')
log_format = os.getenv('KIRIN_LOG_FORMAT', '[%(asctime)s] [%(levelname)5s] [%(process)5s] [%(name)25s] %(message)s')

log_formatter = os.getenv('KIRIN_LOG_FORMATTER', 'default')  # can be 'default' or 'json'

log_extras = json.loads(os.getenv('KIRIN_LOG_EXTRAS', '{}')) # fields to add to the logger

#Log Level available
# - DEBUG
# - INFO
# - WARN
# - ERROR

# logger configuration

LOGGER = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'default': {
            'format': log_format,
        },
        'json': {
            'format': log_format,
            'extras': log_extras,
            '()': 'kirin.utils.CustomJsonFormatter',
        },
    },
    'handlers': {
        'default': {
            'level': log_level,
            'class': 'logging.StreamHandler',
            'formatter': log_formatter,
        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'DEBUG',
            'propagate': False
        },
        'amqp': {
            'level': 'INFO',
        },
        'sqlalchemy.engine': {
            'handlers': ['default'],
            'level': 'WARN',
            'propagate': False
        },
        'sqlalchemy.pool': {
            'handlers': ['default'],
            'level': 'WARN',
            'propagate': False
        },
        'sqlalchemy.dialects.postgresql': {
            'handlers': ['default'],
            'level': 'WARN',
            'propagate': False
        },
        'werkzeug': {
            'handlers': ['default'],
            'level': 'WARN',
            'propagate': False
        },
        'celery.bootsteps': {
            'handlers': ['default'],
            'level': 'WARN',
            'propagate': False
        }
    }
}

CELERYD_HIJACK_ROOT_LOGGER = False
CELERYBEAT_SCHEDULE_FILENAME = '/tmp/celerybeat-schedule-kirin'

CELERYBEAT_SCHEDULE = {
    'poller': {
        'task': 'kirin.tasks.poller',
        'schedule': timedelta(seconds=60),
        'options': {'expires': 30}
    },
}
