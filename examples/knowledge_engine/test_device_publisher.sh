#!/bin/bash

# Script to test the basic functionaly of the knowledge engine adaptor

register_kb() {
    OUT=$(curl -s http://127.0.0.1:8280/rest/sc \
         -X POST \
         -H "Content-Type: application/json" \
         -d @- << EOF
{
    "knowledgeBaseId": "http://example.org/temperature-sensor",
    "knowledgeBaseName": "Test Temperature Sensor",
    "knowledgeBaseDescription": "This is a temperature sensor simulator for testing purposes."
}
EOF
) 2>/dev/null

    echo $?
}

register_ki() {
    OUT=$(curl -s http://127.0.0.1:8280/rest/sc/ki \
         -X POST \
         -H "Content-Type: application/json" \
         -H "Knowledge-Base-Id: http://example.org/temperature-sensor" \
         -d @- << EOF
{
    "knowledgeInteractionType": "PostKnowledgeInteraction",
    "knowledgeInteractionName": "DeviceObservations",
    "argumentGraphPattern": "?sensor rdf:type saref:Sensor .
                             ?measurement saref:measurementMadeBy ?sensor .
                             ?measurement saref:isMeasuredIn saref:TemperatureUnit .
                             ?measurement saref:hasValue ?temperature .
                             ?measurement saref:hasTimestamp ?timestamp .",
    "prefixes":
    {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "saref": "https://saref.etsi.org/core/"
    }
}
EOF
) 2>/dev/null

    echo $?
}

post_data() {
    VALUE="$1"
    TIMESTAMP=$(date +"%Y-%m-%dT%T")

    OUT=$(curl -s http://127.0.0.1:8280/rest/sc/post \
         -X POST \
         -H "Content-Type: application/json" \
         -H "Knowledge-Base-Id: http://example.org/temperature-sensor" \
         -H "Knowledge-Interaction-Id: http://example.org/temperature-sensor/interaction/DeviceObservations"\
         -d @- << EOF
[
    {
        "sensor": "ex:Sensor",
        "measurement": "ex:Measurement",
        "temperature": "\"$VALUE\"^^<http://www.w3.org/2001/XMLSchema#float>",
        "timestamp": "\"$TIMESTAMP\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"
    }
]
EOF
) 2>/dev/null

    echo $?
}

echo "Attempting to register knowledge base"
RET=$(register_kb)
if [ $RET -gt 0 ]
then
    echo "Unable to register knowledge base"

    exit 3
fi

sleep 1
echo "Attempting to register knowledge interaction"
RET=$(register_ki)
if [ $RET -gt 0 ]
then
    echo "Unable to register knowledge interaction"

    exit 4
fi

sleep 1
echo "Attempting to post sensor data"

i=1
while true
do
    if [ $(($i % 120)) -eq 0 ]
    then
        VALUE=999
        echo " Publishing sensor reading: $VALUE °C (simulating faulty sensor)"
    else
        VALUE=$((15 + $RANDOM % 10)).$(($RANDOM % 10))
        echo " $i - Publishing sensor reading: $VALUE °C"
    fi

    out=$(post_data "$VALUE")
    if [ $RET -gt 0 ]
    then
        echo "\nUnable to post sensor data"

        exit 5
    fi

    ((i=i+1))
    sleep 1
done

echo 0
