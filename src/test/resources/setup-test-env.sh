#!/bin/bash
# Sample script to set environment variables for running unit tests
# This setup matches the setup in the test/resources/create-stairwaylib-db.sql
export STAIRWAY_USERNAME='stairwayuser'
export STAIRWAY_PASSWORD='stairwaypw'
export STAIRWAY_URI='jdbc:postgresql://127.0.0.1:5432/stairwaylib'
