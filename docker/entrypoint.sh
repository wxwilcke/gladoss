#!/bin/sh

# which adaptor to use
ADAPTOR=demo

# run the application with the specified arguments
gladoss-run --verbose --endpoint gladoss-demo:8000 --continuous "$ADAPTOR"
