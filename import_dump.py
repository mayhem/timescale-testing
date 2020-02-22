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
                    print("created table")
                    self.conn.commit()
                    break

                except DuplicateTable as err:
                    self.conn.rollback()
                    print("dropped old table")
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


    def insert_files(self, files):

        file_count = len(files)
        listens = []
        done = False
        while not done:
            lowest_ts = datetime.datetime(2038, 1, 1) # lol this will break before time_t rolls over. Ha!
            lowest_index = -1
            last_ts = datetime.datetime(1970, 1, 1)
            for i, f in enumerate(files):
                if f[1][0] < lowest_ts:
                    lowest_index = i
                    lowest_tw = f[1][0]

                    if f[0]:
                        # replace the consumed listen
                        new_listen = self.prepare_listen(f[0])
                        if new_listen:
                            f[1] = new_listen
                        else:
                            f[0] = None
                            file_count -= 1
                            if file_count == 0:
                                done = True
                                break

            assert(lowest_index != -1)
            assert(lowest_ts >= last_ts);
            last_ts = lowest_ts
            listens.append(f[1])

            if len(listens) == 50000:
                self.write_listens(listens)
                listens = []

        self.write_listens(listens)


    def prepare_listen(self, f):

        line = f.readline()
        data = ujson.loads(line)
        tm = data['track_metadata']
        if tm['artist_name']:
            tm['artist_name'] = tm['artist_name'].replace("\u0000", "")
        if tm['track_name']:
             tm['track_name'] = tm['track_name'].replace("\u0000", "")
        if tm['release_name']:
            tm['release_name'] = tm['release_name'].replace("\u0000", "")

        return (datetime.datetime.utcfromtimestamp(data['listened_at']),
             data['recording_msid'],
             data['user_name'],
             ujson.dumps(tm))


    def open_dump_files(self, path):

        files = []
        for filename in os.listdir(path):
            if filename in ['.', '..']:
                continue

            new_path = os.path.join(path, filename)
            if os.path.isdir(new_path):
                files.extend(self.open_dump_files(new_path))

            if filename.endswith(".listens"):
                print("open", new_path)
                f = open(new_path, "r")
                files.append([f, self.prepare_listen(f)])

        return files



@click.command()
@click.argument("listens_dir", nargs=1)
def import_listens(listens_dir):
    with psycopg2.connect('dbname=listenbrainz user=listenbrainz host=localhost password=listenbrainz') as conn:
        li = ListenImporter(conn)
        li.create_tables()
        try:
            files = li.open_dump_files(listens_dir)
        except IOError as err:
            print(err)
            return
        except OSError as err:
            print(err)
            return

        try:
            li.insert_files(files);
        except IOError as err:
            print(err)
            return
        except OSError as err:
            print(err)
            return
        except psycopg2.OperationalError as err:
            print(err)
            return



def usage(command):
    with click.Context(command) as ctx:
        click.echo(command.get_help(ctx))


if __name__ == "__main__":
    import_listens()
    sys.exit(0)
