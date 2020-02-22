#!/usr/bin/env python3

import sys
import click
import os
import ujson


def sort_listens_file(src_file, dest_file):

    print("  read listens")
    listens = []
    while True:
       line = src_file.readline()
       if not line:
           break

       data = ujson.loads(line)
       listens.append((data['listened_at'], line))

    print("  sort listens")
    listens = sorted(listens, key=lambda listen: listen[0])

    print("  save listens")
    for listen in listens:
        dest_file.write(listen[1])



def sort_dump_file(dump_file):

    print(dump_file)
    with open(dump_file, "r") as df:
        with open(dump_file + "-sorted", "w") as sf:
            try:
                sort_listens_file(df, sf)
            except IOError as err:
                print(err)
                return
            except OsError as err:
                print(err)
                return

            os.unlink(dump_file);
            os.rename(dump_file + "-sorted", dump_file)


def sort_dump(dump_dir):

    for filename in os.listdir(dump_dir):
        if filename in ['.', '..']:
            continue

        new_path = os.path.join(dump_dir, filename)
        if os.path.isdir(new_path):
            sort_dump(new_path)

        if filename.endswith(".listens"):
            sort_dump_file(new_path)


if __name__ == "__main__":
    sort_dump(sys.argv[1])
    sys.exit(0)
