#!/bin/bash

echo "-- concat dump"
./concat_dump.py dump complete_dump.json

echo "-- sort dump"
sort --parallel=4 --compress-program=bzip2 complete_dump.json > complete_dump_sorted.json.bz2

echo "-- import dump"
./import_dump.py complete_dump_sorted.json
