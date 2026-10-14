#!/bin/sh

# which adaptor to use
ADAPTOR=restful

# which endpoint to listen to
ENDPOINT=http://gladoss-demo:8000  # docker demo endpoint

# wait a few seconds for the demo device time start posting data
sleep 2

# run the application with the specified arguments
gladoss-run --verbose --endpoint "$ENDPOINT" --continuous "$ADAPTOR"
