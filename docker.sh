#!/bin/bash
set -euox pipefail

NAME="indexd"
PARAM=${1:-DO_NOT_PUSH};

IMAGE_NAME="${DOCKER_RELEASE_REGISTRY:=quay.io}/ncigdc/${NAME}"
REGISTRY="${BASE_CONTAINER_REGISTRY:=quay.io}"

if [ -f 'VERSION.txt' ]; then
  VERSION=$(cat VERSION.txt)
else
  VERSION=$(python -m setuptools_scm)
fi

# setup active branch name, default to using git if build is happening on local
if [ ${TRAVIS_BRANCH+x} ]; then
  GIT_BRANCH=$TRAVIS_BRANCH;
elif [ ${GITLAB_CI+x} ]; then
  GIT_BRANCH=${CI_COMMIT_REF_NAME};
else
  GIT_BRANCH=$(git symbolic-ref --short -q HEAD);
fi

# replace slashes with underscore
GIT_BRANCH=${GIT_BRANCH/\//_}

# Save the commit hash so the /status endpoint doesn't need Git.
COMMIT=$(git rev-parse HEAD)
echo "COMMIT=\"${COMMIT}\"" >indexd/index/version_data.py

BUILD_COMMAND=(build \
  --label org.opencontainers.image.version="${VERSION}" \
  --label org.opencontainers.image.created="$(date -Iseconds)" \
  --label org.opencontainers.image.revision="${COMMIT}" \
  --label org.opencontainers.ref.name="${NAME}:${GIT_BRANCH}" \
  --build-arg REGISTRY="${REGISTRY%\/ncigdc}" \
  --build-arg BASE_VERSION="${BASE_CONTAINER_VERSION:=1.2.0}" \
  --build-arg PIP_INDEX_URL \
  -t "$IMAGE_NAME:$GIT_BRANCH" \
  -t "$IMAGE_NAME:$COMMIT")

docker "${BUILD_COMMAND[@]}" . --progress=plain

if [ "$PARAM" = "push" ]; then
  docker image ls "$IMAGE_NAME"
  docker push -a "$IMAGE_NAME"
fi
