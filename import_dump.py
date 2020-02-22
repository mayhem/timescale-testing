#!/usr/bin/env python3

import sys
import click
import os
import ujson
import psycopg2
from time import time, sleep
from threading import Thread, Lock
from time import time
from psycopg2.errors import OperationalError, DuplicateTable, UntranslatableCharacter
from psycopg2.extras import execute_values

#TODO
# - Take empty fields from influx and not store them in timescale

NUM_THREADS = 6 
NUM_CACHE_ENTRIES = NUM_THREADS * 2
UPDATE_INTERVAL = 500000
BATCH_SIZE = 2000

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

class ListenWriter(Thread):

    def __init__(self, li, conn):
        Thread.__init__(self)

        self.conn = conn
        self.done = False
        self.li = li


    def exit(self):
        self.done = True


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

#        print("Inserted %d rows in %.3f, %d rows/s, ts %d" % (len(listens), t1-t0, int(len(listens)/(t1-t0)), listens[0][0]))


    def run(self):

        while not self.done:
            batch = self.li.get_batch()
            if not batch:
                break

            self.write_listens(batch)



class ListenImporter(object):

    def __init__(self, conn):
        self.total = 0
        self.conn = conn
        self.batches = []
        self.done = False
        self.lock = Lock()
        self.total = 0
        self.t0 = 0


    def exit(self):
        self.done = True


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

    def get_batch(self):
        while True:
            if self.done:
                return None

            self.lock.acquire()
            if len(self.batches):
                listens = self.batches.pop(0)
                self.lock.release()
                return listens

            self.lock.release()
            sleep(.01)


    def add_batch(self, listens):


        if not self.t0:
            self.t0 = time()

        while True:
            if self.done:
                return None

            self.lock.acquire()
            if len(self.batches) >= NUM_CACHE_ENTRIES:
                self.lock.release()
                sleep(.01)
                continue

            self.batches.append(listens)
            self.lock.release()

            self.total += len(listens)
            if self.total % UPDATE_INTERVAL == 0:
                print("queued %d listens. %d rows/s" % (self.total, UPDATE_INTERVAL / (time() - self.t0)))
                self.t0 = time()

            return


    def import_dump_file(self, filename):

        threads = []
        for i in range(NUM_THREADS):
            with psycopg2.connect('dbname=listenbrainz user=listenbrainz host=localhost password=listenbrainz') as conn:
                lw = ListenWriter(self, conn)
                lw.start()
                threads.append(lw)
            

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
                    if last[0] == data['listened_at'] and last[1] == data['recording_msid'] and last[2] == data['user_name']:
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

                if len(listens) == BATCH_SIZE:
                    self.add_batch(listens)
                    listens = []

        if len(listens):
            self.add_batch(listens)

        print("Wait for threads to finish.")
        self.exit()
        for t in threads:
            t.join()

        print("wrote %d listens." % self.total)


@click.command()
@click.argument("listens_file", nargs=1)
def import_listens(listens_file):
    with psycopg2.connect('dbname=listenbrainz user=listenbrainz host=localhost password=listenbrainz') as conn:
        li = ListenImporter(conn)
        try:
            li.create_tables()
        except IOError as err:
            print(err)
            return
        except OSError as err:
            print(err)
            return
        except psycopg2.errors.UntranslatableCharacter:
            print(err)
            return

        try:
            files = li.import_dump_file(listens_file)
        except IOError as err:
            print(err)
            return
        except OSError as err:
            print(err)
            return
        except psycopg2.errors.UntranslatableCharacter:
            print(err)
            return

        try:
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
