#!/bin/sh

# which adaptor to use
ADAPTOR=demo

# which endpoint to listen to
ENDPOINT=http://gladoss-demo:8000  # docker demo endpoint

# run the application with the specified arguments
gladoss-run --verbose --endpoint "$ENDPOINT" --continuous "$ADAPTOR"
