#!/usr/bin/env python3

import sys
import click
import os
import ujson
import psycopg2
import subprocess
import gzip
import datetime
from time import time, sleep
from threading import Thread, Lock
from time import time
from collections import defaultdict
from psycopg2.errors import OperationalError, DuplicateTable, UntranslatableCharacter
from psycopg2.extras import execute_values
import config

NUM_THREADS = 5 
NUM_CACHE_ENTRIES = NUM_THREADS * 2
UPDATE_INTERVAL = 500000
BATCH_SIZE = 2000

CREATE_LISTEN_TABLE_QUERIES = [
"""
    CREATE TABLE listen (
        listened_at     BIGINT                   NOT NULL,
        recording_msid  UUID                     NOT NULL,
        user_name       TEXT                     NOT NULL,
        created         TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        data            JSONB                    NOT NULL
    )
""",
"SELECT create_hypertable('listen', 'listened_at', chunk_time_interval => 432000)",
"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO listenbrainz_ts",
"CREATE OR REPLACE FUNCTION unix_now() returns BIGINT LANGUAGE SQL STABLE as $$ SELECT extract(epoch from now())::BIGINT $$",
"SELECT set_integer_now_func('listen', 'unix_now')",
"""
CREATE VIEW listen_count
       WITH (timescaledb.continuous, timescaledb.refresh_lag=43200, timescaledb.refresh_interval=3600)
         AS SELECT time_bucket(bigint '86400', listened_at) AS listened_at_bucket, user_name, count(listen)
            FROM listen group by time_bucket(bigint '86400', listened_at), user_name;
CREATE VIEW listened_at_max
       WITH (timescaledb.continuous, timescaledb.refresh_lag=43200, timescaledb.refresh_interval=3600)
         AS SELECT time_bucket(bigint '86400', listened_at) AS listened_at_bucket, user_name, max(listened_at) AS max_value
            FROM listen group by time_bucket(bigint '86400', listened_at), user_name;
CREATE VIEW listened_at_min
       WITH (timescaledb.continuous, timescaledb.refresh_lag=43200, timescaledb.refresh_interval=3600)
         AS SELECT time_bucket(bigint '86400', listened_at) AS listened_at_bucket, user_name, min(listened_at) AS min_value
            FROM listen group by time_bucket(bigint '86400', listened_at), user_name;
""",
"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO listenbrainz_ts;"
]

CREATE_INDEX_QUERIES = [
    "CREATE INDEX listened_at_user_name_ndx_listen ON listen (listened_at DESC, user_name)",
    "CREATE UNIQUE INDEX listened_at_recording_msid_user_name_ndx_listen ON listen (listened_at DESC, recording_msid, user_name)"
]

def key_count(listen):
    ''' Return the count of top level keys and track_metadata keys as a rough measure as to which listen
        has more "information".
    '''
    return len(listen.keys()) + len(listen['track_metadata'].keys())


def remove_empty_keys(listen):

    if "track_metadata" in listen:
        listen["track_metadata"] = {k: v for k, v in listen["track_metadata"].items() if v }
        if "additional_info" in listen["track_metadata"]:
            listen["track_metadata"]["additional_info"] = {k: v for k, v in listen["track_metadata"]["additional_info"].items() if v }

    return listen


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
            query = "INSERT INTO listen (listened_at, recording_msid, user_name, data) VALUES %s"
            try:
                t0 = time()
                execute_values(curs, query, listens, template=None)
                self.conn.commit()
                t1 = time()
            except psycopg2.OperationalError as err:
                print("failed to insert rows", err)
                return

        dt = datetime.datetime.fromtimestamp(listens[0][0])
        print("Inserted %d rows in %.3f, %d rows/s, ts %d %d-%02d" % (len(listens), t1-t0, int(len(listens)/(t1-t0)), listens[0][0], dt.year, dt.month))


    def run(self):

        while not self.done:
            batch = self.li.get_batch()
            if batch:
                self.write_listens(batch)
            else:
                sleep(.05)



class ListenImporter(object):

    def __init__(self, conn):
        self.total = 0
        self.conn = conn
        self.batches = []
        self.lock = Lock()
        self.total = 0
        self.t0 = 0
        self.html = None

        self.exact_dup_count = 0
        self.counts = defaultdict(int)


    def create_tables(self):

        with self.conn.cursor() as curs:
            while True:
                try:
                    for query in CREATE_LISTEN_TABLE_QUERIES:
                        curs.execute(query)
                    self.conn.commit()
                    print("created tables")
                    break

                except DuplicateTable as err:
                    self.conn.rollback()
                    print("dropped old table")
                    curs.execute("DROP VIEW listen_count CASCADE")
                    curs.execute("DROP TABLE listen CASCADE")
                    self.conn.commit()



    def create_indexes(self):

        print("create indexes")
        with self.conn.cursor() as curs:
            for query in CREATE_INDEX_QUERIES:
                print(query)
                curs.execute(query)


    def num_batches(self):
        self.lock.acquire()
        batches = len(self.batches)
        self.lock.release()

        return batches


    def get_batch(self):

        self.lock.acquire()
        if len(self.batches):
            listens = self.batches.pop(0)
            self.lock.release()
            return listens

        self.lock.release()
        return None


    def add_batch(self, listens):


        if not self.t0:
            self.t0 = time()

        while True:
            self.lock.acquire()
            if len(self.batches) >= NUM_CACHE_ENTRIES:
                self.lock.release()
                sleep(.01)
                continue

            self.batches.append(listens)
            self.lock.release()

            self.total += len(listens)
            if self.total % UPDATE_INTERVAL == 0:
                print("queued %d listens. %d rows/s" % (self.total, int(UPDATE_INTERVAL / (time() - self.t0))))
                self.t0 = time()

            return



    def cleanup_listen(self, listen):

        tm = listen['track_metadata']

        # Clean up null characters in the data
        if tm['artist_name']:
            tm['artist_name'] = tm['artist_name'].replace("\u0000", "")
        if tm['track_name']:
             tm['track_name'] = tm['track_name'].replace("\u0000", "")
        if tm['release_name']:
            tm['release_name'] = tm['release_name'].replace("\u0000", "")

        return remove_empty_keys(listen)


    def output_duplicate_resolution(self, test, chosen, listen_0, listen_1):
        return 0

        self.html.write("<h2>%s</h2>" % test)
        self.html.write("<h3>Listen 0 - %s</h3><pre>" % ("chosen" if chosen == 0 else "rejected"))
        self.html.write(ujson.dumps(listen_0, indent=4, sort_keys=True))
        self.html.write("</pre><h3>Listen 1 - %s</h3><pre>" % ("chosen" if chosen == 1 else "rejected"))
        self.html.write(ujson.dumps(listen_1, indent=4, sort_keys=True))
        with open("/tmp/a.json", "w") as f:
            f.write(ujson.dumps(listen_0, indent=4, sort_keys=True))
        with open("/tmp/b.json", "w") as f:
            f.write(ujson.dumps(listen_1, indent=4, sort_keys=True))
        diff = subprocess.run(["diff", "/tmp/a.json", "/tmp/b.json"], capture_output=True) 
        self.html.write("</pre><h3>diff</h3><pre>%s</pre>" % diff.stdout.decode("utf-8"))


    def check_for_duplicates(self, listen, lookahead):
        ''' 
            Check for verious types of duplicate tracks. If this track should be inserted
            into the DB, return True. If it should be skipped (e.g. because there is a better 
            match in the lookahead), return False
        '''

        if not len(lookahead):
            return

        # there is weird shit at the start of last.fm. Start checking in 2007
        if listen['listened_at'] > 1167609600:
            tdiff = lookahead[-1]['listened_at'] - listen['listened_at']
            if tdiff <= 2:
                print(lookahead[-1]['listened_at'], listen['listened_at'])

            if tdiff <= 2: 
                print("Possible lookahead underflow, less than 2 seconds in buffer! All good if the process is done! lookahead len: %d" % len(lookahead))

        reached_end_of_la = True
        for i, la_listen in enumerate(lookahead):
            # check for exact duplicate, skip this listen if duplicate
            tm = listen['track_metadata']
            la_tm = la_listen['track_metadata']

#            print("0 %d %s %30s %30s" % (listen['listened_at'],
#                                       listen['recording_msid'][:6], 
#                                       tm['track_name'][:29],
#                                       listen['user_name']))
#            print("1 %d %s %30s %30s" % (la_listen['listened_at'], 
#                                       la_listen['recording_msid'][:6], 
#                                       la_tm['track_name'][:29], 
#                                       la_listen['user_name']))

            # Check to see if recording_msid is the same -- if so, it is a true duplicate
            # which should never happen.
            if listen['listened_at'] == la_listen['listened_at'] and \
                listen['recording_msid'] == la_listen['recording_msid'] and \
                listen['user_name'] == la_listen['user_name']:

                self.output_duplicate_resolution("recording_msid", 1, listen, la_listen)
                self.counts['msid_dup_count'] += 1
#                print("keep 1")

                return 1

            # Check track_name based duplicates and pick best listen to keep
            if listen['listened_at'] == la_listen['listened_at'] and \
                listen['user_name'] == la_listen['user_name'] and \
                tm['track_name'].lower() == la_tm['track_name'].lower():


                # If we have a dedup tag in the lookahead listen, it seems to have more
                # infomation, so remove the dedup_tag field and keep that version
                if 'dedup_tag' in la_tm["additional_info"]:
                    self.counts['dedup_tag_count'] += 1
                    del lookahead[i]
                    self.output_duplicate_resolution("dedup_tag 0", 1, listen, la_listen)
#                    print("keep 0")
                    return 0
                if 'dedup_tag' in tm["additional_info"]:
                    self.counts['dedup_tag_count'] += 1
                    self.output_duplicate_resolution("dedup_tag 1", 0, listen, la_listen)
#                    print("keep 1")
                    return 1

                self.counts['track_name_dup_count'] += 1
                if key_count(listen) > key_count(la_listen):
                    self.output_duplicate_resolution("track_name", 0, listen, la_listen)
                    del lookahead[i]
#                    print("keep 0")
                    return 0

                self.output_duplicate_resolution("track_name", 1, listen, la_listen)
#                print("keep 1")
                return 1

            # Check to see if two listens have a listen timestamps less than 3 seconds apart
            if abs(listen['listened_at'] - la_listen['listened_at']) <= 3 and \
                listen['user_name'] == la_listen['user_name'] and \
                tm['track_name'].lower() == la_tm['track_name'].lower():

                self.counts['fuzzy_dup_count'] += 1
                if key_count(listen) > key_count(la_listen):
                    self.output_duplicate_resolution("fuzzy timestamp", 0, listen, la_listen)
                    del lookahead[i]
#                    print("keep 0")
                    return 0

                self.output_duplicate_resolution("fuzzy timestamp", 1, listen, la_listen)
#                print("keep 1")
                return 1


            if la_listen['listened_at'] > listen['listened_at'] + 5:
                break

#        print("keep both")

        return 2


    def import_dump_file(self, filename):

        self.html = open("output.html", "w")
        self.html.write('<!doctype html>')
        self.html.write('<html lang="en">')
        self.html.write("<head>\n")
        self.html.write('<meta charset="utf-8">')
        self.html.write('</head><body>\n')

        threads = []
        for i in range(NUM_THREADS):
            with psycopg2.connect(config.DB_CONNECT) as conn:
                lw = ListenWriter(self, conn)
                lw.start()
                threads.append(lw)
            

        print("import ", filename)
        NUM_LOOKAHEAD_SECONDS = 2
        lookahead = []
        listens = []
        with gzip.open(filename, "rb") as f:
            while True:
                while len(lookahead) == 0 or (lookahead[-1]['listened_at'] - lookahead[0]['listened_at']) <= NUM_LOOKAHEAD_SECONDS:
                   line = f.readline()
                   if not line:
                       break
              
                   ts, jsdata = line.decode('utf-8').split('-', 1)
                   listen = self.cleanup_listen(ujson.loads(jsdata))
              
                   # Check for invalid timestamps (last.fm got started in 2004 or so!)
                   if listen['listened_at'] < 1136073600: # Jan 1 2006
                       continue

                   lookahead.append(listen)
              
                if not len(lookahead):
                    break

                listen = lookahead.pop(0)
                ret = self.check_for_duplicates(listen, lookahead)
                if ret == 0:
                   lookahead.insert(0, listen)
                elif ret == 2:
                   listens.append([
                       listen['listened_at'],
                       listen['recording_msid'],
                       listen['user_name'],
                       ujson.dumps(listen)])
              
                if len(listens) == BATCH_SIZE:
                    self.add_batch(listens)
                    listens = []


        assert(len(lookahead) == 0)
        if len(listens):
            self.add_batch(listens)

        print("Wait for batches to write")
        while self.num_batches() > 0:
            sleep(1)
        
        print("Wait for threads to finish.")
        for t in threads:
            t.exit()
        for t in threads:
            t.join()

        print("wrote %d listens." % self.total)
        self.html.write("<p>Counts:<pre>%s</pre></p>\n" % ujson.dumps(self.counts, indent=4, sort_keys=True))
        self.html.write("</body></html>")
        self.html.close()


@click.command()
@click.argument("listens_file", nargs=1)
def import_listens(listens_file):
    with psycopg2.connect(config.DB_CONNECT) as conn:
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
