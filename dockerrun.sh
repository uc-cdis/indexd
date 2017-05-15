#!/bin/sh
sed -i -e "s/worker_processes auto;/worker_processes ${NGINX_PROCESSES:-auto};/g" /etc/nginx/nginx.conf
sed -i -e "s/processes = 1/processes = ${UWSGI_PROCESSES:-1}/g" /etc/uwsgi/uwsgi.ini
cd /var/www/indexd; sudo -u nginx python wsgi.py
uwsgi --ini /etc/uwsgi/uwsgi.ini --uid nginx &
nginx
