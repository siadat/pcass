#!/bin/sh
set -e
ts=/work/tree-sitter/target/release/tree-sitter
for file in $(find /work/cassandra -name '*.java' | grep -iv test); do
  xml_name=$(echo $file | sed 's/\.java$/.java.xml/')
  echo "Parsing $file"
  echo "     -> $xml_name"
  $ts parse --xml $file > $xml_name
done
