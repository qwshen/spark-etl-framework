#!/bin/bash

# Please run a docker container with Confluent 4.1.2 installed with the following changes, then have it up and running
# In server.properties (/opt/confluent/etc/kafka/server.properties)
#   - advertised.listeners=PLAINTEXT://localhost:9092
#   - listener.security.protocol.map=PLAINTEXT:PLAINTEXT

# --- create topic - users **********************
kafka-topics --zookeeper localhost:2181 --list --topic "users" | grep "users"
if [ $? != 0 ]; then
  kafka-topics --zookeeper localhost:2181 --create --topic users --partitions 3 --replication-factor 1
fi

# --- create topic - train **********************
kafka-topics --zookeeper localhost:2181 --list --topic "train" | grep "train"
if [ $? != 0 ]; then
  kafka-topics --zookeeper localhost:2181 --create --topic train --partitions 3 --replication-factor 1
fi

# --- create topic - events **********************
kafka-topics --zookeeper localhost:2181 --list --topic "events" | grep "events"
if [ $? != 0 ]; then
  kafka-topics --zookeeper localhost:2181 --create --topic events --partitions 3 --replication-factor 1
fi

# --- create topic - features **********************
kafka-topics --zookeeper localhost:2181 --list --topic "features" | grep "features"
if [ $? != 0 ]; then
  kafka-topics --zookeeper localhost:2181 --create --topic features --partitions 3 --replication-factor 1
fi

# --- create topic - stream-features **********************
kafka-topics --zookeeper localhost:2181 --list --topic "stream-features" | grep "stream-features"
if [ $? != 0 ]; then
  kafka-topics --zookeeper localhost:2181 --create --topic stream-features --partitions 3 --replication-factor 1
fi

# --- list all topics
kafka-topics --zookeeper localhost:2181 --list


# register users-key schema
curl -X GET "http://localhost:8081/subjects" | grep "users-key"
if [ $? == 0 ]; then
  curl -X DELETE "http://localhost:8081/subjects/users-key"
fi
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"type\": \"string\"}"}' "http://localhost:8081/subjects/users-key/versions"

# register users-value schema
curl -X GET "http://localhost:8081/subjects" | grep "users-value"
if [ $? == 0 ]; then
  curl -X DELETE "http://localhost:8081/subjects/users-value"
fi
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"namespace\": \"com.it21learning.streaming.model\", \"name\": \"User\", \"type\": \"record\", \"fields\": [{\"name\": \"user_id\", \"type\": \"string\"}, {\"name\": \"birthyear\", \"type\": [\"null\", \"string\"]}, {\"name\": \"gender\", \"type\": [\"null\", \"string\"]}, {\"name\": \"joinedAt\", \"type\": [\"null\", \"string\"]}]}"}' "http://localhost:8081/subjects/users-value/versions"

# register events-key schema
curl -X GET "http://localhost:8081/subjects" | grep "events-key"
if [ $? == 0 ]; then
  curl -X DELETE "http://localhost:8081/subjects/events-key"
fi
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"type\": \"string\"}"}' "http://localhost:8081/subjects/events-key/versions"

# register train-value schema
curl -X GET "http://localhost:8081/subjects" | grep "events-value"
if [ $? == 0 ]; then
  curl -X DELETE "http://localhost:8081/subjects/events-value"
fi
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"namespace\": \"com.it21learning.streaming.model\", \"name\": \"Events\", \"type\": \"record\", \"fields\": [{\"name\": \"event_id\", \"type\": [\"null\", \"string\"]}, {\"name\": \"user_id\", \"type\": [\"null\", \"string\"]}, {\"name\": \"start_time\", \"type\": [\"null\", \"string\"]}, {\"name\": \"city\", \"type\": [\"null\", \"string\"]}, {\"name\": \"province\", \"type\": [\"null\", \"string\"]}, {\"name\": \"country\", \"type\": [\"null\", \"string\"]}]}"}' "http://localhost:8081/subjects/events-value/versions"
