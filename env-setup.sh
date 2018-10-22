#!/bin/bash
# USAGE: source env-setup.sh

set -ex

CLUSTER_NAME=${HAIL_CLUSTER_NAME:-cs-test}
echo "configuring environment for ${CLUSTER_NAME} (override by setting HAIL_CLUSTER_NAME)"

PLATFORM="${HAIL_PLATFORM:-${OSTYPE}}"

case "$PLATFORM" in
    darwin*)
        install-docker() {
            brew cask install docker
            open /Applications/Docker.app
        }
        install-conda() {
            tmpfile=$(mktemp /tmp/abc-script.XXXXXX)
            curl -L https://repo.continuum.io/miniconda/Miniconda3-latest-MacOSX-x86_64.sh > $tmpfile
            bash $tmpfile
        }
        install-gcloud() {
            brew cask install gcloud
        }
        ;;
    linux*)
        install-docker() {
            echo "installing docker on $PLATFORM is unsupported, please manually install: https://docs.docker.com/install/linux/docker-ce/ubuntu"
        }
        install-conda() {
            tmpfile=$(mktemp /tmp/abc-script.XXXXXX)
            curl -L https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh > $tmpfile
            bash $tmpfile
        }
        install-gcloud() {
            apt-get install gcloud || yum install gcloud
        }
        ;;
    *)
        echo "unsupported platform $PLATFORM if you think this is the wrong platform, explicitly set $HAIL_PLATFORM"
        ;;
esac

docker version || install-docker
conda -V || install-conda
gcloud -v || install-gcloud

PROJECT_NAME=$(gcloud config get-value project)
if [[ "$PROJECT_NAME" == "(unset)" ]]
then
    echo "no project configured, will set config to the default project: broad-ctsa"
    gcloud config set project broad-ctsa
    PROJECT_NAME="broad-ctsa"
fi

if [[ "$(gcloud config get-value compute/region)" == "(unset)" ]]
then
    echo "no compute/region configured, will set config to the default region: us-central"
    gcloud config set compute/region us-central
fi

kubectl version -c || gcloud components install kubectl
kubectl version || gcloud container clusters get-credentials $CLUSTER_NAME

for project in "$(cat projects.txt)"
do
    conda env create -f $project/environment.yml || conda env update -f $project/environment.yml
    [[ ! -e $project/env-setup.sh ]] || source $project/env-setup.sh
done

SVC_ACCT_NAME=${HAIL_SVC_ACCT_NAME:-$(whoami)-gke}
KEY_FILE=~/.hail-dev/gke/svc-acct/${SVC_ACCT_NAME}.json
echo "Configuring service account with short name ${SVC_ACCT_NAME} with key file stored in ${KEY_FILE}"
if [ ! -e "${KEY_FILE}" ]
then
    echo "${KEY_FILE} not found, will attempt to create a key file (and user if necessary)"
    gcloud iam service-accounts describe ${SVC_ACCT_NAME} || gcloud iam service-accounts create ${SVC_ACCT_NAME}
    gcloud projects add-iam-policy-binding \
           ${PROJECT_NAME} \
           --member "serviceAccount:${SVC_ACCT_NAME}@${PROJECT_NAME}.iam.gserviceaccount.com" \
           --role "roles/owner"
    mkdir -p $(dirname ${KEY_FILE})
    gcloud iam service-accounts keys create \
           ${KEY_FILE} \
           --iam-account ${SVC_ACCT_NAME}@${PROJECT_NAME}.iam.gserviceaccount.com
fi

export GOOGLE_APPLICATION_CREDENTIALS="${KEY_FILE}"
