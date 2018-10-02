#!/bin/bash
set -ex

kubectl get --selector=app=hail-ci deployments -o "jsonpath={.items[*].metadata.labels.hail\.is/sha}"
