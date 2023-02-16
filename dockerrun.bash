#!/bin/bash

sed -i.bak -e 's/WSGIDaemonProcess indexd processes=1 threads=3/WSGIDaemonProcess indexd processes='${WSGI_PROCESSES:-1}' threads='${WSGI_THREADS:-3}'/g' /etc/apache2/sites-available/apache-indexd.conf
cd /var/www/indexd
#
# Update certificate authority index -
# environment may have mounted more authorities
#
update-ca-certificates
#
# Enable debug flag based on GEN3_DEBUG environment
#
if [[ -f ./wsgi.py && "$GEN3_DEBUG" == "True" ]]; then
  echo -e "\napplication.debug=True\n" >> ./wsgi.py
fi

sudo -u www-data python3 wsgi.py
/usr/sbin/apache2ctl -D FOREGROUND
