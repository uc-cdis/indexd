#!/bin/bash

nginx
gunicorn -c "/indexd/deployment/wsgi/gunicorn.conf.py"
