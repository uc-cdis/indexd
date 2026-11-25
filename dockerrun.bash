#!/bin/bash

# A volume mounted over /var/lib/nginx for a read-only root starts empty.
mkdir -p /var/lib/nginx/tmp && nginx -e stderr || exit $?
exec gunicorn --no-control-socket -c "/indexd/deployment/wsgi/gunicorn.conf.py"
