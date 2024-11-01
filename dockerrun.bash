#!/bin/bash
source .venv/bin/activate
nginx
poetry run gunicorn -c "/indexd/deployment/wsgi/gunicorn.conf.py"
