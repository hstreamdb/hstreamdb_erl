#! /usr/bin/env bash

docker stop hserver-test0
docker stop hserver-test1
docker stop hserver-test2

docker stop hstore-test
docker stop zookeeper-test

docker rm hserver-test0
docker rm hserver-test1
docker rm hserver-test2

docker rm hstore-test
docker rm zookeeper-test

rm -rf /tmp/hstream-test-data
