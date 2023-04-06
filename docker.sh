#!/bin/bash
set -eou pipefail

PARAM=${1:-push};

DEFAULT_REGISTRY="${DOCKER_RELEASE_REGISTRY:=quay.io/ncigdc}"

# avoid installing git
COMMIT=$(git rev-parse HEAD) && echo "COMMIT=\"${COMMIT}\"" >indexd/index/version_data.py
VERSION=$(git describe --always --tags) && echo "VERSION=\"${VERSION}\"" >>indexd/index/version_data.py
GIT_BRANCH=${TRAVIS_BRANCH:~CI_COMMIT_BRANCH}
IMAGE_NAME=${DEFAULT_REGISTRY}/indexd

# setup active branch name, default to using git if build is happening on local
if [ -z ${GIT_BRANCH+x} ]; then
  GIT_BRANCH=$(git symbolic-ref --short -q HEAD);
elif [ -z ${GIT_BRANCH+x} ]; then
  GIT_BRANCH=$TRAVIS_BRANCH;
else
  GIT_BRANCH=$CI_COMMIT_BRANCH;
fi

# replace slashes with underscore
GIT_BRANCH=${GIT_BRANCH/\//_}

echo "$VERSION"

docker build --build-arg version="$VERSION" --ssh default -t "$IMAGE_NAME:$GIT_BRANCH" .

if [ "$PARAM" = "push" ]; then
  docker push "$IMAGE_NAME:$GIT_BRANCH"
fi
