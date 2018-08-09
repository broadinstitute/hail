.PHONY: hail-ci-build-image push-hail-ci-build-image
.DEFAULT_GOAL := default

hail-ci-build-image: GIT_SHA = $(shell git rev-parse HEAD)
hail-ci-build-image:
	docker build . -t hail-pr-builder/0.1:${GIT_SHA} -f Dockerfile.pr-builder

push-hail-ci-build-image: GIT_SHA = $(shell git rev-parse HEAD)
push-hail-ci-build-image: hail-ci-build-image
	docker tag hail-pr-builder/0.1:${GIT_SHA} gcr.io/broad-ctsa/hail-pr-builder/0.1:${GIT_SHA}
	docker push gcr.io/broad-ctsa/hail-pr-builder/0.1
	echo gcr.io/broad-ctsa/hail-pr-builder/0.1:${GIT_SHA} > hail-ci-build-image

default:
	echo Do not use this makefile to build hail, for information on how to \
	     build hail see: https://hail.is/docs/devel/
	exit -1
