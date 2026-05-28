#!/bin/sh

# which adaptor to use
ADAPTOR=restful

# run the application with the specified arguments
gladoss-run -v -v --continuous --pattern-decay=120 --samplesize=30 "$ADAPTOR"
