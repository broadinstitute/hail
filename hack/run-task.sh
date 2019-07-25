#!/bin/bash
set -ex

# wait for docker to be active
while true; do
    # systemctl won't report active until we run a docker command
    docker ps -a || true
    if [ $(systemctl is-active docker) == "active" ]; then
        break
    fi
    sleep 1
done

docker info

mkdir /shared

TOKEN=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/token")

gsutil cp gs://hail-cseed/cs-hack/tmp/$TOKEN/config.json /config.json

python3 /run-task.py >run-task2.out 2>run-task2.err
echo $? > run-task2.ec

gsutil cp run-task2.out run-task2.err run-task2.ec gs://hail-cseed/cs-hack/tmp/$TOKEN/

# terminate
export NAME=$(curl http://metadata.google.internal/computeMetadata/v1/instance/name -H 'Metadata-Flavor: Google')
export ZONE=$(curl http://metadata.google.internal/computeMetadata/v1/instance/zone -H 'Metadata-Flavor: Google')
gcloud -q compute instances delete $NAME --zone=$ZONE
