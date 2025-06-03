#!/bin/bash

# Prerequisite: yq installed (https://mikefarah.gitbook.io/yq/)
# Usage: ./run-task.sh anova-task.yaml

CONFIG_FILE="$1"

if [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: YAML file not found: $CONFIG_FILE"
  exit 1
fi

MAIN_CLASS=$(yq '.mainClass' "$CONFIG_FILE")
JAR_PATH=$(yq '.jarPath' "$CONFIG_FILE")
PARAMS=$(yq -o=json '.params' "$CONFIG_FILE" | jq -r 'to_entries | map("\(.key)=\(.value)") | join(" ")')

/opt/spark/bin/spark-submit \
  --class "$MAIN_CLASS" \
  "$JAR_PATH" \
  $PARAMS
