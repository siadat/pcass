#!/bin/bash
set -e
docker build -f Dockerfile -t javaparser .
echo "Image built..." >&2
docker run -it --rm javaparser parse --xml /dev/stdin
