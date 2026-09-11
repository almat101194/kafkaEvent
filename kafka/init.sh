#!/bin/bash

setup() {
  local default_options="--bootstrap-server localhost:9092 --command-config /opt/bitnami/kafka/config/command-config.properties"

  kafka-topics.sh $default_options --list
  # Создать топик
  kafka-topics.sh $default_options --create --partitions 3 --replication-factor 1 --topic ets.antifraud-event --if-not-exists
  # На случай, если топик уже существовал (например, с прошлого раза) с меньшим числом партиций:
  # --if-not-exists выше в этом случае ничего не сделает, поэтому доводим partitions до 3 явно
  # (Kafka разрешает только увеличивать число партиций, не уменьшать)
  kafka-topics.sh $default_options --alter --partitions 3 --topic ets.antifraud-event
  # Добавить пользователя equifax-transfer-service
  kafka-configs.sh $default_options --alter --entity-type users --entity-name equifax-transfer-service --add-config 'SCRAM-SHA-256=[password=equifax-transfer-service],SCRAM-SHA-512=[password=equifax-transfer-service]'
  # Дать права пользователю equifax-transfer-service на топик ets.antifraud-event
  kafka-acls.sh $default_options --add --allow-principal User:equifax-transfer-service --operation ALL --topic ets.antifraud-event
}

setup &