#!/bin/bash
set -ex

gsutil -m cp gs://hail-common/dev/pipeline/run-worker.sh gs://hail-common/dev/pipeline/worker.py /

nohup /bin/bash run-worker.sh >run-worker.log 2>&1 &
