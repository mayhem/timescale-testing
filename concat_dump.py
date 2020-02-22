#!/usr/bin/env python3

import sys
import os
import ujson


def output_dump_file(outf, dump_file):

    print(dump_file)
    with open(dump_file, "r") as src_file:
        while True:
            line = src_file.readline()
            if not line:
                break

            data = ujson.loads(line)
            out = ("%012d-" % data['listened_at']) + line
            outf.write(out)


def concat_dump(f, dump_dir, output_file):

    opened = False
    if not f:
        f = open(output_file, "w")
        opened = False

    for filename in os.listdir(dump_dir):
        if filename in ['.', '..']:
            continue

        new_path = os.path.join(dump_dir, filename)
        if os.path.isdir(new_path):
            concat_dump(f, new_path, output_file)

        if filename.endswith(".listens"):
            output_dump_file(f, new_path)

    if opened:
        f.close()


if __name__ == "__main__":
    concat_dump(None, sys.argv[1], sys.argv[2])
    sys.exit(0)
