FROM ubuntu:15.04
MAINTAINER CDIS <cdissupport@opensciencedatacloud.org>

RUN apt-get update && apt-get install -y python-pip git python-dev postgresql postgresql-client libpq-dev apache2 libapache2-mod-wsgi \ 
 && apt-get clean && apt-get autoremove \
 && rm -rf /var/lib/apt/lists/*
ADD . /indexd
RUN cd /indexd && pip install .

RUN mkdir -p /var/www/indexd/ && cp /indexd/wsgi.py /var/www/indexd/wsgi.py && cp /indexd/bin/indexd /var/www/indexd/indexd

RUN echo '<VirtualHost *:80>\n\
    WSGIDaemonProcess indexd processes=1 threads=3 python-path=/var/www/indexd/:/usr/bin/python\n\
    WSGIProcessGroup indexd\n\
    WSGIScriptAlias / /var/www/indexd/wsgi.py\n\
    DocumentRoot /var/www/indexd/\n\
    <Directory "/var/www/indexd/">\n\
        Header set Access-Control-Allow-Origin "*"\n\
        WSGIProcessGroup indexd\n\
        WSGIApplicationGroup %{GLOBAL}\n\
        Options +ExecCGI\n\
        Order deny,allow\n\
        Allow from all\n\
    </Directory>\n\
    ErrorLog ${APACHE_LOG_DIR}/error.log\n\
    LogLevel warn\n\
    CustomLog ${APACHE_LOG_DIR}/access.log combined\n\
</VirtualHost>\n'\
>> /etc/apache2/sites-available/apache-indexd.conf

RUN a2ensite apache-indexd
RUN a2enmod headers
RUN a2dissite 000-default.conf

EXPOSE 80

WORKDIR /var/www/indexd

CMD  /usr/sbin/apache2ctl -D FOREGROUND
