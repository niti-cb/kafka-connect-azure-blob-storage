#!/bin/bash

export PARTITIONS=5
export ITERATIONS=20000
export FLUSH=1000
export DIRECTORY="system-test"

echo "============================================================================================================================="
echo "                           TEST CASE: SCHEMA DRIVEN DATA CONVERSION WITH JSON SERIALIZATION CLASS                            "
echo "============================================================================================================================="

export TOPIC="json-messages-distributed"
export SOURCE_CONNECTOR_NAME="json-source-distributed"
export SINK_CONNECTOR_NAME="json-sink-distributed"
export CONSUMER_GROUP="connect-$SINK_CONNECTOR_NAME"
export VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter"

sh ./shell_scripts/test.sh
echo "============================================================================================================================="

echo "\n"

echo "============================================================================================================================="
echo "                           TEST CASE: SCHEMA DRIVEN DATA CONVERSION WITH AVRO SERIALIZATION CLASS                            "
echo "============================================================================================================================="

export TOPIC="avro-messages-distributed"
export SOURCE_CONNECTOR_NAME="avro-source-distributed"
export SINK_CONNECTOR_NAME="avro-sink-distributed"
export CONSUMER_GROUP="connect-$SINK_CONNECTOR_NAME"
export VALUE_CONVERTER="io.confluent.connect.avro.AvroConverter"

sh ./shell_scripts/test.sh
echo "============================================================================================================================="

echo "\n"

echo "============================================================================================================================="
echo "                                                  TEST CASE: CHAOS TESTING                                                   "
echo "============================================================================================================================="

export TOPIC="chaos-messages-distributed"
export SOURCE_CONNECTOR_NAME="chaos-source-distributed"
export SINK_CONNECTOR_NAME="chaos-sink-distributed"
export CONSUMER_GROUP="connect-$SINK_CONNECTOR_NAME"
export VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter"
export CHAOS=true

sh ./shell_scripts/test.sh
echo "============================================================================================================================="
