#!/bin/bash

sed -i.bak -e 's/WSGIDaemonProcess indexd processes=1 threads=3/WSGIDaemonProcess indexd processes='${WSGI_PROCESSES:-1}' threads='${WSGI_THREADS:-3}'/g' /etc/apache2/sites-available/apache-indexd.conf
cd /var/www/indexd; sudo -u www-data python wsgi.py
/usr/sbin/apache2ctl -D FOREGROUND
