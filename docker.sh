#!/bin/bash
set -x

PARAM=${1:-push};

#DEFAULT_REGISTRY="quay.io"

# avoid installing git
COMMIT=$(git rev-parse HEAD) && echo "COMMIT=\"${COMMIT}\"" >indexd/index/version_data.py
VERSION=$(git describe --always --tags) && echo "VERSION=\"${VERSION}\"" >>indexd/index/version_data.py
GIT_BRANCH=$(git symbolic-ref --short -q HEAD);

IMAGE_NAME=quay.io/ncigdc/indexd

# replace slashes with underscore
GIT_BRANCH=${GIT_BRANCH/\//_}

echo "$VERSION"
echo "$GIT_BRANCH"

docker build --build-arg version="$VERSION" --ssh default -t "$IMAGE_NAME:$GIT_BRANCH" .

if [ "$PARAM" = "push" ]; then
  docker push "$IMAGE_NAME:$GIT_BRANCH"
fi
