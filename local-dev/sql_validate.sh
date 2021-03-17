#!/bin/bash

# validate mysql
echo "sleeping for 5 seconds during postgres boot..."
sleep 5
export PGPASSWORD=stairwaypw
psql --username stairwayuser --host=postgres --port=5432 -d stairwaylib -c "SELECT VERSION();SELECT NOW()"
