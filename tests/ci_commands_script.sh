#!/usr/bin/env bash

poetry run pytest -vv --cov=indexd --cov-append --cov-report xml tests
