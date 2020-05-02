How to use an LB dump to kick-start a timescale database
=======================================================

./concat_dump <listen_dump> unsorted_listens.json
gzip unsorted_listens.json
zcat unsorted_listens.json.gz | sort --parallel=16 --compress-program=gzip > sorted_listens.json
import_dump.py sorted_listens.json
