#!/bin/bash

nginx
exec gunicorn -c "/indexd/deployment/wsgi/gunicorn.conf.py"
