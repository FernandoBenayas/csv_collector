#!/bin/bash

echo Checking if elasticsearch is up and listening...
ES_URL="http://localhost:9200/_cat/indices?v"
es_request=$(curl $ES_URL)

while [[ $es_request != *"simulation"* ]]
do
es_request=$(curl $ES_URL)
echo Elasticsearch is still sleeping...
/bin/sleep 2
done

echo You used pókeflute. Elasticsearch just woke up.

/bin/sleep 2

python /home/script/csv_collector.py