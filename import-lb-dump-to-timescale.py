#!/usr/bin/env python3

import sys
import click
import os
import ujson
import psycopg2
import datetime
from time import time
from psycopg2.errors import OperationalError, DuplicateTable, UntranslatableCharacter
from psycopg2.extras import execute_values
from influxdb import InfluxDBClient

#TODO
# - Take empty fields from influx and not store them in timescale

CREATE_LISTEN_TABLE_QUERY = """
    CREATE TABLE listen (
        listened_at     TIMESTAMPTZ       NOT NULL,
        recording_msid  UUID              NOT NULL,
        user_name       TEXT              NOT NULL,
        data            JSONB             NOT NULL
    )
"""

def create_tables(conn):

    with conn.cursor() as curs:
        while True:
            try:
                curs.execute(CREATE_LISTEN_TABLE_QUERY)
                curs.execute("SELECT create_hypertable('listen', 'listened_at')")
                conn.commit()
                break

            except DuplicateTable as err:
                conn.rollback()
                curs.execute("DROP TABLE listen")
                conn.commit()

total = 0
def write_listens(conn, listens):

    global total

    with conn.cursor() as curs:
        query = "INSERT INTO listen VALUES %s"
        try:
            t0 = time()
            execute_values(curs, query, listens, template=None)
            conn.commit()
            t1 = time()
        except psycopg2.OperationalError as err:
            print("failed to insert rows", err)
            return

    total += len(listens)
    print("Inserted %d rows in %.3f, %d rows/s. Total %d" % (len(listens), t1-t0, int(len(listens)/(t1-t0)), total))


def parse_listens(conn, filename):

    print("import ", filename)
    listens = []
    with open(filename, "rt") as f:
         while True:
            line = f.readline()
            if not line:
                break

            data = ujson.loads(line)
            tm = data['track_metadata']
            if tm['artist_name']:
                tm['artist_name'] = tm['artist_name'].replace("\u0000", "")
            if tm['track_name']:
                 tm['track_name'] = tm['track_name'].replace("\u0000", "")
            if tm['release_name']:
                tm['release_name'] = tm['release_name'].replace("\u0000", "")
            listens.append([
                datetime.datetime.utcfromtimestamp(data['listened_at']),
                data['recording_msid'],
                data['user_name'],
                ujson.dumps(tm)])
            if len(listens) == 50000:
                try:
                    write_listens(conn, listens)
                except psycopg2.errors.UntranslatableCharacter:
                    print(listens)
                listens = []

    write_listens(conn, listens)


def get_measurement_name(user_name):
    """ Function to return the measurement name that influx has saved for given user name"""

    # Note: there are we have to replace each \ with two backslashes because influxdb-python
    # adds an extra backslash for each backslash in the measurement name itself
    return '"{}"'.format(user_name.replace('\\', '\\\\').replace('\n', '\\\\n'))


def get_escaped_measurement_name(user_name):
    """ Function to return the string which can directly be passed into influx queries for a
        user's measurement
    """

    # Note: influxdb-python first replaces each backslash in the username with two backslashes
    # and because in influx queries, we have to escape each backslash, overall each backslash
    # must be replaced by 4 backslashes. Yes, this is hacky and ugly.
    return '"\\"{}\\""'.format(user_name.replace('\\', '\\\\\\\\').replace('"', '\\"').replace('\n', '\\\\\\\\n'))


def fetch_listens(iclient, conn, user_name):

    query = 'SELECT * FROM ' + get_escaped_measurement_name(user_name) + ' ORDER BY time ASC'
    try:
        results = iclient.query(query)
    except Exception as err:
        self.log.error("Cannot query influx while getting listens for user: %s: %s", user_name, str(err), exc_info=True)
        return []

    listens = []
    for result in results.get_points(measurement=get_measurement_name(user_name)):
        print(result)



@click.command()
def import_listens():
    with psycopg2.connect('dbname=listenbrainz user=listenbrainz host=localhost password=listenbrainz') as conn:
        create_tables(conn)
        print("connect to influx")
        iclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'listenbrainz')
        print("connected to influx")
        fetch_listens(iclient, conn, "rob")


def usage(command):
    with click.Context(command) as ctx:
        click.echo(command.get_help(ctx))


if __name__ == "__main__":
    import_listens()
    sys.exit(0)
