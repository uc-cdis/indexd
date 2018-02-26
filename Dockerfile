FROM ubuntu:16.04
MAINTAINER CDIS <cdissupport@opensciencedatacloud.org>

RUN apt-get update && apt-get install -y sudo python-pip git python-dev libpq-dev apache2 libapache2-mod-wsgi vim libssl-dev libffi-dev \ 
 && apt-get clean && apt-get autoremove \
 && rm -rf /var/lib/apt/lists/*
COPY . /indexd
WORKDIR /indexd
RUN COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >indexd/index/version_data.py
RUN VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>indexd/index/version_data.py
RUN python setup.py install

RUN mkdir -p /var/www/indexd/ && chmod 777 /var/www/indexd && cp /indexd/wsgi.py /var/www/indexd/wsgi.py && cp /indexd/bin/indexd /var/www/indexd/indexd

#
# Custom apache2 logging - see http://www.loadbalancer.org/blog/apache-and-x-forwarded-for-headers/
#
RUN echo '<VirtualHost *:80>\n\
    WSGIDaemonProcess indexd processes=1 threads=3 python-path=/var/www/indexd/:/usr/bin/python home=/var/www/indexd\n\
    WSGIScriptAlias / /var/www/indexd/wsgi.py\n\
    WSGIPassAuthorization On\n\
    DocumentRoot /var/www/indexd/\n\
    <Directory "/var/www/indexd/">\n\
        Header set Access-Control-Allow-Origin "*"\n\
        WSGIApplicationGroup %{GLOBAL}\n\
        Options +ExecCGI\n\
        Order deny,allow\n\
        Allow from all\n\
    </Directory>\n\
    ErrorLog ${APACHE_LOG_DIR}/error.log\n\
    LogLevel warn\n\
    LogFormat "%{X-Forwarded-For}i %l %{X-UserId}i %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"" aws\n\
    SetEnvIf X-Forwarded-For "^..*" forwarded\n\
    CustomLog ${APACHE_LOG_DIR}/access_log combined env=!forwarded\n\
    CustomLog ${APACHE_LOG_DIR}/access.log aws env=forwarded\n\
</VirtualHost>\n'\
>> /etc/apache2/sites-available/apache-indexd.conf

RUN a2ensite apache-indexd
RUN a2enmod headers
RUN a2dissite 000-default.conf

EXPOSE 80

WORKDIR /var/www/indexd


RUN ln -sf /dev/stdout /var/log/apache2/access.log && ln -sf /dev/stderr /var/log/apache2/error.log
CMD  rm -f /var/run/apache2/apache2.pid && /indexd/dockerrun.bash
