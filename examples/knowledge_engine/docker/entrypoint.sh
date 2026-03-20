#!/bin/sh

# which adaptor to use
ADAPTOR=knowledge_engine

# run the application with the specified arguments
gladoss-run -v -v --report-level=3 --continuous "$ADAPTOR"
