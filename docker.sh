#!/bin/bash
set -euo pipefail

PARAM="${1:-push}";

# avoid installing git
COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >indexd/index/version_data.py
VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>indexd/index/version_data.py

IMAGE_NAME="quay.io/ncigdc/indexd"

# setup active branch name, default to using git if build is happening on local
if [ -z ${TRAVIS_BRANCH+x} ]; then
  GIT_BRANCH=$(git symbolic-ref --short -q HEAD);
else
  GIT_BRANCH=$TRAVIS_BRANCH;
fi

# replace slashes with underscore
GIT_BRANCH=${GIT_BRANCH/\//_}

echo "$VERSION"

docker build --build-arg version="$VERSION" --ssh default -t "$IMAGE_NAME:$GIT_BRANCH" .

if [ "$PARAM" = "push" ]; then
  docker push "$IMAGE_NAME:$GIT_BRANCH"
fi
