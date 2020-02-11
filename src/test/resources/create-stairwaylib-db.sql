-- Sample SQL to create a local Stairway database for stairway unit testing
-- This setup matches the setup in the test/resources/setup-test-env.sh
create user stairwayuser WITH PASSWORD 'stairwaypw';
create database stairwaylib;
grant all privileges on database stairwaylib to stairwayuser;