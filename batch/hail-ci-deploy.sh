set -ex

cd batch

gcloud -q auth activate-service-account \
  --key-file=/secrets/gcr-push-service-account-key.json

gcloud -q auth configure-docker

SHA=$(git rev-parse --short=12 HEAD)

# get sha label of batch deployment
DEPLOYED_SHA=$(kubectl get --selector=app=batch deployments -o "jsonpath={.items[*].metadata.labels.hail\.is/sha}")

if [[ $(git cat-file -t "$DEPLOYED_SHA" 2>/dev/null || true) == commit ]]; then
    if [[ "$SHA" == "$DEPLOYED_SHA" ]]; then
	exit 0
    fi

    NEEDS_REDEPLOY=$(cd $ROOT && python3 project-changed.py $DEPLOYED_SHA batch)
    if [[ $NEEDS_REDEPLOY = no ]]; then
        exit 0
    fi
fi

# requires docker
make push-batch

sed -e "s,@sha@,$SHA," \
    -e "s,@image@,$(cat batch-image)," \
    < deployment.yaml.in > deployment.yaml

kubectl apply -f deployment.yaml

# ci can't recover from batch restart yet
kubectl delete pods -l app=hail-ci
