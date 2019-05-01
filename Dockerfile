# To run: docker run -v /path/to/wsgi.py:/var/www/indexdz/wsgi.py --name=indexdz -p 81:80 indexdz
# To check running container: docker exec -it indexdz /bin/bash 

FROM quay.io/cdis/py27base:pybase2-1.0.0

MAINTAINER CDIS <cdissupport@opensciencedatacloud.org>

RUN mkdir /var/www/indexd \
	&& chown www-data /var/www/indexd

COPY . /indexd
COPY ./deployment/uwsgi/uwsgi.ini /etc/uwsgi/uwsgi.ini

WORKDIR /indexd

# RUN apk update && apk add --no-cache sudo git python-dev  nginx  curl vim libffi-dev
# COPY . /indexd
RUN python -m pip install -r requirements.txt
RUN COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >indexd/index/version_data.py
RUN VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>indexd/index/version_data.py
RUN python setup.py install

# RUN mkdir -p /var/www/indexd/ && chmod 777 /var/www/indexd && cp /indexd/wsgi.py /var/www/indexd/wsgi.py && cp /indexd/bin/indexd /var/www/indexd/indexd

#
# Custom apache2 logging - see http://www.loadbalancer.org/blog/apache-and-x-forwarded-for-headers/
#
# COPY ./deployment/apache-indexd.conf /etc/apache2/sites-available/apache-indexd.conf

# RUN a2ensite apache-indexd
# RUN a2enmod headers
# RUN a2enmod reqtimeout
# RUN a2dissite 000-default.conf

EXPOSE 80

WORKDIR /var/www/indexd


# RUN ln -sf /dev/stdout /var/log/nginx/access.log && ln -sf /dev/stderr /var/log/nginx/error.log
# CMD  rm -rf /var/run/apache2/apache2.pid && /indexd/dockerrun.bash

CMD /dockerrun.sh

# plan
# nginx
# uwsgi 
# 	this requires uwsgi.ini and uwsgi.conf