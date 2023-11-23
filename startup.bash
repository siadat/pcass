#!/bin/bash
set -e

# Wait for Cassandra to be ready
until cqlsh -e "DESCRIBE KEYSPACES" > /dev/null; do
  echo "Cassandra is unavailable - sleeping"
  sleep 1
done

# Cassandra is ready - execute the CQL file
cqlsh -f /root/work/populate_rows.cql
