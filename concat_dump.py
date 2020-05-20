#!/usr/bin/env python3

import sys
import os
import tarfile
import ujson
import gzip


def output_dump_file(outf, src_file):

    while True:
        line = src_file.readline()
        if not line:
            break

        line = line.decode("utf-8")
        data = ujson.loads(line)

        # skip listens with a 0 timestamp
        if data['listened_at'] == 0:
            continue

        out = ("%012d-" % data['listened_at']) + line
        outf.write(bytes(out, 'utf-8'))


def concat_dump(dump_file, output_file):

    with gzip.open(output_file, 'wb') as outfile:
        with tarfile.open(dump_file, 'r:xz') as tar:
            for tarinfo in tar:
                if tarinfo.isreg() and tarinfo.name.endswith(".listens"):
                    print(tarinfo.name)
                    output_dump_file(outfile, tar.extractfile(tarinfo))


if __name__ == "__main__":
    concat_dump(sys.argv[1], sys.argv[2])
    sys.exit(0)
