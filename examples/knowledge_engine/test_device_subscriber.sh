#!/bin/bash

# Script to test the basic functionaly of the knowledge engine adaptor

register_kb() {
    OUT=$(curl -s http://127.0.0.1:8280/rest/sc \
         -X POST \
         -H "Content-Type: application/json" \
         -d @- << EOF
{
    "knowledgeBaseId": "http://example.org/receiver",
    "knowledgeBaseName": "Test Receiver",
    "knowledgeBaseDescription": "This is a receiver for testing purposes."
}
EOF
) 2>/dev/null

    echo $?
}

register_ki() {
    OUT=$(curl -s http://127.0.0.1:8280/rest/sc/ki \
         -X POST \
         -H "Content-Type: application/json" \
         -H "Knowledge-Base-Id: http://example.org/receiver" \
         -d @- << EOF
{
    "knowledgeInteractionType": "ReactKnowledgeInteraction",
    "knowledgeInteractionName": "AnomalyReceiver",
    "argumentGraphPattern": ?report rdf:type sh:ValidationReport .
                            ?report dct:date ?reportDate .
                            ?report dct:identifier ?reportIdentifier .
                            ?report dct:conformsTo ?reportLanguage .
                            ?report sh:conforms ?validationPassed .
                            ?report dct:hasPart ?result .

                            ?result rdf:type sh:ValidationResult .
                            ?result rdfs:label ?resultStatusMsg .
                            ?result sh:focusNode ?resultFocusNode .
                            ?result sh:resultPath ?resultPath .
                            ?result sh:value ?resultValue .
                            ?result sh:sourceShape ?resultSourceShape .
                            ?result sh:resultMessage ?resultStatusMsgLong .
                            ?result sh:resultSeverity ?resultSeverity .

                            ?resultSeverity rdf:type sh:Severity .
                            ?resultSeverity rdfs:label ?severityLabel .
                            ?resultSeverity rdfs:comment ?severityDescription .",
    "prefixes":
    {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "dct": "http://purl.org/dc/terms/",
        "sh": "http://www.w3.org/ns/shacl#"
    }
}
EOF
) 2>/dev/null

    echo $?
}

post_receipt() {
    REQ_ID="$1"

    OUT=$(curl -s http://127.0.0.1:8280/rest/sc/post \
         -X POST \
         -H "Content-Type: application/json" \
         -H "Knowledge-Base-Id: http://example.org/receiver" \
         -H "Knowledge-Interaction-Id: http://example.org/receiver/interaction/AnomalyReceiver"\
         -d @- << EOF
{
    "handleRequestId": "$REQ_ID",
    "bindingSet: []
}
EOF
) 2>/dev/null

    echo $?
}

poll_endpoint() {
    OUT=$(curl -s http://127.0.0.1:8280/rest/sc/handle \
         -X GET \
         -H "Knowledge-Base-Id: http://example.org/receiver" \
         ) 2>/dev/null

    echo "$OUT"
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
echo "listening for messages"

i=1
while true
do 
    RES=$(poll_endpoint)
    if [ -z "$RES" ]
    then
        echo "\nProblem with receiving messages"

        exit 6
    fi

    valid_response=false
    case "$RES" in
        *handleRequestId* )
            valid_response=true
            ;;
    esac

    if [ "$valid_response" = false ]
    then
        echo "Connection reset. Retrying"
        continue
    fi

    REQ_ID=$(echo "$RES" | jq -r '.handleRequestId')
    RET=$(post_receipt "$REQ_ID")
    if [ $RET -gt 0 ]
    then
        echo "\nUnable to post receipt"

        exit 5
    fi

    BS=$(echo "$RES" | jq -r '.bindingSet')
    echo "$i - Received message\n$BS"

    ((i=i+1))
done

echo 0
