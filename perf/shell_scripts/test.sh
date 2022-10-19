#!/bin/bash

export KAFKA_BOOTSTRAP_SERVERS="http://localhost:9093"
export SOURCE_CONNECT="http://localhost:8084"
export SINK_CONNECT="http://localhost:8083"
export SCHEMA_REGISTRY="http://schema-registry:8081"

if type "jq" &> /dev/null; then
  alias prettyjson="jq ."
elif type "python" &> /dev/null; then
  alias prettyjson="python -m json.tool"
elif type "python3" &> /dev/null; then
    alias prettyjson="python3 -m json.tool"
else
  alias prettyjson="cat"
fi

SOURCE_JSON="{
  \"name\": \"$SOURCE_CONNECTOR_NAME\",
  \"config\": {
    \"connector.class\": \"io.confluent.kafka.connect.datagen.DatagenConnector\",
    \"kafka.topic\": \"$TOPIC\",
    \"quickstart\": \"product\",
    \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
    \"value.converter\": \"$VALUE_CONVERTER\",
    \"value.converter.schema.registry.url\": \"$SCHEMA_REGISTRY\",
    \"max.interval\": 10,
    \"iterations\": \"$ITERATIONS\",
    \"tasks.max\": 1
  }
}"

SINK_JSON="{
  \"name\": \"$SINK_CONNECTOR_NAME\",
  \"config\": {
    \"connector.class\": \"io.coffeebeans.connect.azure.blob.sink.AzureBlobSinkConnector\",
    \"tasks.max\": \"$PARTITIONS\",
    \"topics\": \"$TOPIC\",
    \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
    \"value.converter\": \"$VALUE_CONVERTER\",
    \"value.converter.schema.registry.url\": \"$SCHEMA_REGISTRY\",
    \"azblob.connection.string\": \"$AZURE_STORAGE_CONNECTION_STRING\",
    \"azblob.container.name\": \"$AZURE_CONTAINER\",
    \"format\": \"JSON\",
    \"topics.dir\": \"$DIRECTORY\",
    \"flush.size\": \"$FLUSH\"
  }
}"

echo "Creating topic..."
docker exec -it kafka kafka-topics.sh --create --topic $TOPIC --partitions $PARTITIONS --bootstrap-server=$KAFKA_BOOTSTRAP_SERVERS
echo "-----------------------------------------------------------------------------------------------------------------------------"

echo "Creating source connector..."
curl -X POST -H "Content-Type: application/json" -d "$SOURCE_JSON" "$SOURCE_CONNECT/connectors" | prettyjson
echo "Created source connector."
echo "-----------------------------------------------------------------------------------------------------------------------------"

echo "Creating sink connector..."
curl -X POST -H "Content-Type: application/json" -d "$SINK_JSON" "$SINK_CONNECT/connectors" | prettyjson
echo "Created sink connector."
echo "-----------------------------------------------------------------------------------------------------------------------------"

echo "Sleeping for 1 minute..."
sleep 60
echo "-----------------------------------------------------------------------------------------------------------------------------"

if [ "$CHAOS" = true ] ; then
  echo "Sink Connector Tasks -"
  curl "$SINK_CONNECT/connectors/$SINK_CONNECTOR_NAME/status" | prettyjson
  echo "-----------------------------------------------------------------------------------------------------------------------------"
  echo "Bringing down kafka-connect workers 02 and 03..."
  docker stop kafka-connect-02
  docker stop kafka-connect-03
  echo "Done."
  echo "-----------------------------------------------------------------------------------------------------------------------------"
  echo "Waiting for tasks to be rebalanced...\n"
  sleep 20
  echo "Sink Connector Tasks (Rebalanced) -"
  curl "$SINK_CONNECT/connectors/$SINK_CONNECTOR_NAME/status" | prettyjson
  echo "-----------------------------------------------------------------------------------------------------------------------------"
fi

echo "Waiting for consumer..."
docker exec -it kafka-system-test python -m python_scripts.wait_to_consume --topic=$TOPIC --consumer_group=$CONSUMER_GROUP
echo "-----------------------------------------------------------------------------------------------------------------------------"

echo "Deleting connectors..."
curl -X DELETE "$SOURCE_CONNECT/connectors/$SOURCE_CONNECTOR_NAME"
curl -X DELETE "$SINK_CONNECT/connectors/$SINK_CONNECTOR_NAME"
echo "Deleted connectors."
echo "-----------------------------------------------------------------------------------------------------------------------------"

echo "Sleeping for 1 minute..."
sleep 60
echo "-----------------------------------------------------------------------------------------------------------------------------"

echo "Running tests..."
docker exec -it kafka-system-test python -m python_scripts.run_tests --topic=$TOPIC --directory=$DIRECTORY
echo "-----------------------------------------------------------------------------------------------------------------------------"

echo "Deleting topic..."
docker exec -it kafka bash -c "kafka-topics.sh --delete --topic $TOPIC --bootstrap-server=$KAFKA_BOOTSTRAP_SERVERS"
echo "Deleted topic."
echo "-----------------------------------------------------------------------------------------------------------------------------"
