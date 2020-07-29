#!/bin/bash
set -euo pipefail

rm -rf build/src
mkdir -p build/src

pip install \
    --user \
    --src build/src \
    --no-deps \
    $(grep '^-e.*egg=' requirements.txt)

cp requirements.txt build/requirements.txt
sed -i.tmp \
  -e 's;^-e.*egg=;build/src/;' \
build/requirements.txt


# avoid installing git
COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >indexd/index/version_data.py
VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>indexd/index/version_data.py

# Build and push docker image.
docker build -t ${REGISTRY}/${REPO}:${GIT_TAG} . --build-arg http_proxy=https://cloud-proxy:3128 --build-arg https_proxy=https://cloud-proxy:3128
docker push ${REGISTRY}/${REPO}:${GIT_TAG}