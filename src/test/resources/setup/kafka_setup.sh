#!/bin/bash

# set up docker kafka so as to be able to be connected from outside
# in server.properties (/opt/confluent/etc/kafka/server.properties)
# advertised.listeners=PLAINTEXT://localhost:9092
# listener.security.protocol.map=PLAIN:PLAINTEXT

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
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"namespace\": \"com.it21learning.streaming.model\", \"name\": \"User\", \"type\": \"record\", \"fields\": [{\"name\": \"user_id\", \"type\": \"string\"}, {\"name\": \"locale\", \"type\": [\"null\", \"string\"]}, {\"name\": \"birthyear\", \"type\": [\"null\", \"string\"]}, {\"name\": \"gender\", \"type\": [\"null\", \"string\"]}, {\"name\": \"joinedAt\", \"type\": [\"null\", \"string\"]}, {\"name\": \"location\", \"type\": [\"null\", \"string\"]}, {\"name\": \"timezone\", \"type\": [\"null\", \"string\"]}]}"}' "http://localhost:8081/subjects/users-value/versions"

# register train-key schema
curl -X GET "http://localhost:8081/subjects" | grep "train-key"
if [ $? == 0 ]; then
  curl -X DELETE "http://localhost:8081/subjects/train-key"
fi
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"type\": \"string\"}"}' "http://localhost:8081/subjects/train-key/versions"

# register train-value schema
curl -X GET "http://localhost:8081/subjects" | grep "train-value"
if [ $? == 0 ]; then
  curl -X DELETE "http://localhost:8081/subjects/train-value"
fi
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"schema": "{\"namespace\": \"com.it21learning.streaming.model\", \"name\": \"Train\", \"type\": \"record\", \"fields\": [{\"name\": \"user\", \"type\": [\"null\", \"string\"]}, {\"name\": \"event\", \"type\": [\"null\", \"string\"]}, {\"name\": \"invited\", \"type\": [\"null\", \"string\"]}, {\"name\": \"timestamp\", \"type\": [\"null\", \"string\"]}, {\"name\": \"interested\", \"type\": [\"null\", \"string\"]}, {\"name\": \"not_interested\", \"type\": [\"null\", \"string\"]}]}"}' "http://localhost:8081/subjects/train-value/versions"


# produce sample messages
kafka-avro-console-producer --broker-list localhost:9092 --topic users --property parse.key=true --property=key.schema="$(curl http://localhost:8081/subjects/users-key/versions/1 | jq -r .schema)" --property value.schema="$(curl http://localhost:8081/subjects/users-value/versions/4 | jq -r .schema)"
# paste result as follows
# "john smith" (tab) {"name": "john smith", "age": 31, "gender": "male", "title": "manager", "department": "IT Development", "webUrl": "http://www.abc.com"}

kafka-avro-console-producer --broker-list localhost:9092 --topic users --property value.schema="$(curl http://localhost:8081/subjects/users-value/versions/4 | jq -r .schema)"




