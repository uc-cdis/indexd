#!/usr/bin/env bash
set -e

echo "Creating indexd_tests database..."
psql -U postgres -h localhost -d postgres -c "CREATE DATABASE indexd_tests;" || true

poetry run pytest -vv --cov=indexd --cov=migrations/versions --cov-append --cov-report xml tests
