# To run: docker run -v /path/to/wsgi.py:/var/www/indexd/wsgi.py --name=indexd -p 81:80 indexd
# To check running container: docker exec -it indexd /bin/bash 

FROM quay.io/cdis/py27base:pybase2-1.0.1

MAINTAINER CDIS <cdissupport@opensciencedatacloud.org>

RUN mkdir /var/www/indexd \
	&& chown www-data /var/www/indexd

COPY . /indexd
COPY ./deployment/uwsgi/uwsgi.ini /etc/uwsgi/uwsgi.ini

WORKDIR /indexd

RUN python -m pip install -r requirements.txt
RUN COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >indexd/index/version_data.py
RUN VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>indexd/index/version_data.py
RUN python setup.py install


EXPOSE 80

WORKDIR /var/www/indexd
RUN cp /indexd/wsgi.py .

CMD /dockerrun.sh
