#!/bin/bash


exec uvicorn indexd.app:app_init --host 0.0.0.0 --port 8000 --timeout-graceful-shutdown 90

# nginx
# poetry run gunicorn -c "/indexd/deployment/wsgi/gunicorn.conf.py"
