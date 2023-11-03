#!/usr/bin/env bash

poetry run pytest -vv --cov=indexd --cov-report xml tests
