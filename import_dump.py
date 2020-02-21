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

class ListenImporter(object):

    def __init__(self, conn):
        self.total = 0
        self.conn = conn


    def create_tables(self):

        with self.conn.cursor() as curs:
            while True:
                try:
                    curs.execute(CREATE_LISTEN_TABLE_QUERY)
                    curs.execute("SELECT create_hypertable('listen', 'listened_at')")
                    self.conn.commit()
                    break

                except DuplicateTable as err:
                    self.conn.rollback()
                    curs.execute("DROP TABLE listen")
                    self.conn.commit()


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


    def parse_listens_file(self, filename):

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


    def write_chunk(self, chunk):
        FINISH ME!

        for blah blah:
            if len(listens) == 50000:
                try:
                    self.write_listens(listens)
                except psycopg2.errors.UntranslatableCharacter:
                    print(listens)
                listens = []

        self.write_listens(listens)


    def collect_listens(self, path):

        for chunk in num_chunks:
            for filename in os.listdir(path):
                if filename in ['.', '..']:
                    continue

                new_path = os.path.join(path, filename)
                if os.path.isdir(new_path):
                    self.find_listen_files(self.conn, new_path)

                if filename.endswith(".listens"):
                    self.parse_listens(self.conn, new_path)


    def import_dir(self, listens_dir, num_chunks):

        chunks = [ {} for i in range(num_chunks) ]





@click.command()
@click.argument("listens_dir", nargs=1)
@click.argument("chunks", nargs=1)
def import_listens(listens_dir, chunks):
    with psycopg2.connect('dbname=listenbrainz user=listenbrainz host=localhost password=listenbrainz') as conn:
        li = ListenImporter(conn)
        li.create_tables()
        li.import_dir(listens_dir, chunks)


def usage(command):
    with click.Context(command) as ctx:
        click.echo(command.get_help(ctx))


if __name__ == "__main__":
    import_listens()
    sys.exit(0)
