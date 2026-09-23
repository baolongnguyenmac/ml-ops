#!/bin/bash
path=$1

curl -s -X POST -H 'Content-Type: application/json' \
    http://localhost:8083/connectors \
    -d @$path | python3 -m json.tool
