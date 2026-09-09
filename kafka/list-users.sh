#!/bin/bash

kafka-configs.sh --bootstrap-server localhost:9092 \
    --command-config /opt/bitnami/kafka/config/command-config.properties \
    --describe --entity-type users