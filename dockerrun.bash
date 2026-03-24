#!/bin/bash

nginx
poetry run gunicorn -c "/indexd/deployment/wsgi/gunicorn.conf.py"
