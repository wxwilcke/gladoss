#!/bin/sh

# which adaptor to use
ADAPTOR=knowledge_engine

# run the application with the specified arguments
gladoss-run -v -v --report-level=3 --continuous --no-evaluate-structure --pattern-decay=120 --samplesize=30 "$ADAPTOR"
