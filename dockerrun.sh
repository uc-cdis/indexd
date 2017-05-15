#!/bin/sh
cd /var/www/indexd; sudo -u nginx python wsgi.py
uwsgi --ini /etc/uwsgi/uwsgi.ini --uid nginx &
nginx
