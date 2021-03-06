FROM python:2.7-onbuild

RUN pip install gunicorn
RUN apt-get update && apt-get install -y protobuf-compiler
WORKDIR /usr/src/app

# pg client is needed to test the postgres cnx
RUN apt-get install -y postgresql-client

RUN python setup.py build_version
RUN python setup.py build_pbf

CMD python ./manage.py db upgrade; gunicorn -b 0.0.0.0:9090 --access-logfile - kirin:app

