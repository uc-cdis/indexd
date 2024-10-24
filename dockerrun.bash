#!/bin/bash
source .venv/bin/activate
nginx
gunicorn -c "/indexd/deployment/wsgi/gunicorn.conf.py"
