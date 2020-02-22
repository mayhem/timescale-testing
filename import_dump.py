#!/usr/bin/env python3

import sys
import click
import os
import ujson
import psycopg2
from time import time
from psycopg2.errors import OperationalError, DuplicateTable, UntranslatableCharacter
from psycopg2.extras import execute_values

#TODO
# - Take empty fields from influx and not store them in timescale

CREATE_LISTEN_TABLE_QUERY = """
    CREATE TABLE listen (
        listened_at     BIGINT            NOT NULL,
        recording_msid  UUID              NOT NULL,
        user_name       TEXT              NOT NULL,
        data            JSONB             NOT NULL
    )
"""

CREATE_INDEX_QUERIES = [
    "CREATE INDEX ON listen (listened_at DESC, user_name)"
]

class ListenImporter(object):

    def __init__(self, conn):
        self.total = 0
        self.conn = conn


    def create_tables(self):

        with self.conn.cursor() as curs:
            while True:
                try:
                    curs.execute(CREATE_LISTEN_TABLE_QUERY)
                    curs.execute("SELECT create_hypertable('listen', 'listened_at', chunk_time_interval => %s)" % (86400 * 5))
                    print("created table")
                    self.conn.commit()
                    break

                except DuplicateTable as err:
                    self.conn.rollback()
                    print("dropped old table")
                    curs.execute("DROP TABLE listen")
                    self.conn.commit()



    def create_indexes(self):

        print("create indexes")
        with self.conn.cursor() as curs:
            for query in CREATE_INDEX_QUERIES:
                print(query)
                curs.execute(query)


    def write_listens(self, listens):

        with self.conn.cursor() as curs:
            query = "INSERT INTO listen VALUES %s"
            try:
                t0 = time()
                execute_values(curs, query, listens, template=None)
                self.conn.commit()
                t1 = time()
            except psycopg2.OperationalError as err:
                print("failed to insert rows", err)
                return

        self.total += len(listens)
        print("Inserted %d rows in %.3f, %d rows/s. Total %d" % (len(listens), t1-t0, int(len(listens)/(t1-t0)), self.total))


    def import_dump_file(self, filename):

        print("import ", filename)
        listens = []
        last = None
        with open(filename, "rt") as f:
             while True:
                line = f.readline()
                if not line:
                    break

                ts, jsdata = line.split('-', 1)
                data = ujson.loads(jsdata)
                tm = data['track_metadata']

                # Clean up null characters in the data
                if tm['artist_name']:
                    tm['artist_name'] = tm['artist_name'].replace("\u0000", "")
                if tm['track_name']:
                     tm['track_name'] = tm['track_name'].replace("\u0000", "")
                if tm['release_name']:
                    tm['release_name'] = tm['release_name'].replace("\u0000", "")

                # check for duplicate listens
                if last:
                    if last[0] == data['listened_at'] and last[1] == data['recording_msid'] and last[2] = data['user_name']:
                        continue # its a dup
                last = (data['listened_at'], data['recording_msid'], data['user_name'])

                # Check for 0 timestamps and skip them
                if data['listened_at'] == 0:
                    continue

                listens.append([
                    data['listened_at'],
                    data['recording_msid'],
                    data['user_name'],
                    ujson.dumps(tm)])
                if len(listens) == 50000:
                    self.write_listens(listens)
                    listens = []

        self.write_listens(listens)


@click.command()
@click.argument("listens_file", nargs=1)
def import_listens(listens_file):
    with psycopg2.connect('dbname=listenbrainz user=listenbrainz host=localhost password=listenbrainz') as conn:
        li = ListenImporter(conn)
        try:
            li.create_tables()
            files = li.import_dump_file(listens_file)
            li.create_indexes()
        except IOError as err:
            print(err)
            return
        except OSError as err:
            print(err)
            return
        except psycopg2.errors.UntranslatableCharacter:
            print(err)
            return


def usage(command):
    with click.Context(command) as ctx:
        click.echo(command.get_help(ctx))


if __name__ == "__main__":
    import_listens()
    sys.exit(0)
