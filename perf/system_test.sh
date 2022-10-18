#!/bin/bash

source .env.systemtest

docker-compose -f docker-compose-base.yml up -d --wait
if [ "$?" -ne 0 ]; then
  exit
fi

echo
echo "#############################################################################################################################"
echo "                                                SYSTEM TEST: STANDALONE MODE                                                 "
echo "#############################################################################################################################"

docker-compose -f docker-compose-standalone.yml up -d --wait
if [ "$?" -ne 0 ]; then
  exit
fi
sh ./shell_scripts/test_cases_standalone.sh
docker-compose -f docker-compose-standalone.yml down

echo
echo "#############################################################################################################################"
echo "                                                SYSTEM TEST: DISTRIBUTED MODE                                                "
echo "#############################################################################################################################"

docker-compose -f docker-compose-distributed.yml up -d --wait
if [ "$?" -ne 0 ]; then
  exit
fi
sh ./shell_scripts/test_cases_distributed.sh
docker-compose -f docker-compose-distributed.yml down

docker-compose -f docker-compose-base.yml down

echo "#############################################################################################################################"
