#!/bin/bash

exec uvicorn indexd.app:get_app --factory --host 0.0.0.0 --port 8000 --timeout-graceful-shutdown 90
