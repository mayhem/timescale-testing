#!/bin/bash

echo "-- concat dump"
./concat_dump.py dump complete_dump.json

echo "-- sort dump"
sort complete_dump.json > complete_dump_sorted.json
rm complete_dump.json

echo "-- import dump"
./import_dump.py complete_dump_sorted.json
