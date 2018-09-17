#!/bin/sh

set -e

echo_build_properties() {
  echo "[Build Metadata]"
  echo user=$USER
  echo revision=$(git rev-parse HEAD)
  echo branch=$(git rev-parse --abbrev-ref HEAD)
  echo date=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo url=$(git config --get remote.origin.url)
  echo sparkVersion=$1
  echo hailVersion=$2
}

mkdir -p src/main/resources/

echo_build_properties $1 $2 > "src/main/resources/build-info.properties"
python scripts/parse_version_info.py src/main/resources/build-info.properties python/hail/_generated_version_info.py
