#!/bin/bash
set -euo pipefail

# avoid installing git
COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >indexd/index/version_data.py
VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>indexd/index/version_data.py

docker build -t indexd .

# If this is not a pull request, update the branch's docker tag.
if [ $TRAVIS_PULL_REQUEST = 'false' ]; then
  docker tag indexd quay.io/ncigdc/indexd:${TRAVIS_BRANCH/\//_} \
    && docker push quay.io/ncigdc/indexd:${TRAVIS_BRANCH/\//_};

  # If this commit has a tag, use on the registry too.
  if ! test -z $TRAVIS_TAG; then
    docker tag indexd quay.io/ncigdc/indexd:${TRAVIS_TAG} \
      && docker push quay.io/ncigdc/indexd:${TRAVIS_TAG};
  fi
fi
