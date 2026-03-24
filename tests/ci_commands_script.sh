#!/usr/bin/env bash

poetry run pytest -vv --cov=indexd --cov=migrations/versions --cov-append --cov-report xml tests
