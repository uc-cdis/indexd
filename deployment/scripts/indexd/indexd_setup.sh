#!/bin/bash
# entrypoint bash script for indexd

## try to install python util, or else fallback to environment variables.

# activate virtual env
pushd /indexd
# TODO: check if poetry can install globally
python -m venv py-venv && . py-venv/bin/activate && source $HOME/.poetry/env
poetry install -vv --no-dev --no-interaction && poetry show -v
python3 -m pip install cdispyutils
popd

# TODO: check if the metrics endpoint is up before curling.
# This is a workaround for scraping stdout for metrics.
/dockerrun.sh & while true; do curl -X GET ${METRICS_URL}; sleep ${METRICS_CHECK_INTERVAL}; done
