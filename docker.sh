#!/bin/bash
set -eo pipefail

PARAM=${1:-push};

IMAGE_NAME="${DOCKER_RELEASE_REGISTRY:=quay.io}/ncigdc/indexd"

# avoid installing git
COMMIT=$(git rev-parse HEAD) && echo "COMMIT=\"${COMMIT}\"" >indexd/index/version_data.py

# setup active branch name, default to using git if build is happening on local
if [ ${TRAVIS_BRANCH+x} ]; then
  GIT_BRANCH=$TRAVIS_BRANCH;
elif [ ${GITLAB_CI+x} ]; then
  GIT_BRANCH=${CI_COMMIT_REF_SLUG}
else
  GIT_BRANCH=$(git symbolic-ref --short -q HEAD);
fi

# replace slashes with underscore
GIT_BRANCH=${GIT_BRANCH/\//_}

VERSION=$(cat VERSION.txt)
echo "$VERSION"

BUILD_COMMAND=(build \
  --label org.opencontainers.image.version="${VERSION}" \
  --label org.opencontainers.image.created="$(date -Iseconds)" \
  --label org.opencontainers.image.revision="$(git rev-parse HEAD)" \
  --label org.opencontainers.ref.name="indexd:${GIT_BRANCH}" \
  --ssh default -t "$IMAGE_NAME:$GIT_BRANCH")

echo "${BUILD_COMMAND[@]}"

docker "${BUILD_COMMAND[@]}" .

if [ "$PARAM" = "push" ]; then
  docker push "$IMAGE_NAME:$GIT_BRANCH"
fi
