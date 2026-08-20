#!/usr/bin/env bash

uv run pytest -vv --cov=indexd --cov=migrations/versions --cov-append --cov-report xml tests
