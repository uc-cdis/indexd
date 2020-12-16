# To run: docker run -v /path/to/wsgi.py:/var/www/indexd/wsgi.py --name=indexd -p 81:80 indexd
# To check running container: docker exec -it indexd /bin/bash

FROM quay.io/cdis/python-nginx:pybase3-1.4.1


ENV appname=indexd

RUN apk update \
    && apk add postgresql-libs postgresql-dev libffi-dev libressl-dev \
    && apk add linux-headers musl-dev gcc \
    && apk add curl bash git vim

COPY . /$appname
COPY ./deployment/uwsgi/uwsgi.ini /etc/uwsgi/uwsgi.ini
COPY ./deployment/uwsgi/wsgi.py /$appname/wsgi.py
WORKDIR /$appname

RUN python -m pip install --upgrade pip \
    && python -m pip install --upgrade setuptools \
    && pip install -r requirements.txt --use-deprecated=legacy-resolver

RUN mkdir -p /var/www/$appname \
    && mkdir -p /var/www/.cache/Python-Eggs/ \
    && mkdir /run/nginx/ \
    && ln -sf /dev/stdout /var/log/nginx/access.log \
    && ln -sf /dev/stderr /var/log/nginx/error.log \
    && chown nginx -R /var/www/.cache/Python-Eggs/ \
    && chown nginx /var/www/$appname

EXPOSE 80

RUN COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >$appname/index/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>$appname/index/version_data.py \
    && python setup.py install

WORKDIR /var/www/$appname

CMD /dockerrun.sh
