#!/bin/bash

docker volume create --driver local --name timescale
docker run --detach -p 5432:5432 -v timescale:/var/lib/postgresql/data --name timescale-db timescale/timescaledb:latest-pg11
