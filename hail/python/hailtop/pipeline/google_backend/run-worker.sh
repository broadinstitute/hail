#!/bin/bash
set -x

python3 -m pip install -U uvloop

export CORES=$(nproc)
export DRIVER_BASE_URL=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/driver_base_url")
export SCRATCH=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/scratch")
export INST_TOKEN=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/inst_token")

python3 /worker.py

gsutil -m cp worker.log run-worker.log $SCRATCH/worker_logs/$INST_TOKEN/

export NAME=$(curl http://metadata.google.internal/computeMetadata/v1/instance/name -H 'Metadata-Flavor: Google')
export ZONE=$(curl http://metadata.google.internal/computeMetadata/v1/instance/zone -H 'Metadata-Flavor: Google')
gcloud -q compute instances delete $NAME --zone=$ZONE
