FROM ubuntu:16.04
MAINTAINER CDIS <cdissupport@opensciencedatacloud.org>

RUN apt-get update && apt-get install -y sudo python-pip git python-dev libpq-dev apache2 libapache2-mod-wsgi curl vim libssl-dev libffi-dev \ 
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
COPY ./deployment/apache-indexd.conf /etc/apache2/sites-available/apache-indexd.conf

RUN a2ensite apache-indexd
RUN a2enmod headers
RUN a2enmod reqtimeout
RUN a2dissite 000-default.conf

EXPOSE 80

WORKDIR /var/www/indexd


RUN ln -sf /dev/stdout /var/log/apache2/access.log && ln -sf /dev/stderr /var/log/apache2/error.log
CMD  rm -rf /var/run/apache2/apache2.pid && /indexd/dockerrun.bash
