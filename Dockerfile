# To run: docker run -v /path/to/wsgi.py:/var/www/indexd/wsgi.py --name=indexd -p 81:80 indexd
# To check running container: docker exec -it indexd /bin/bash

FROM quay.io/cdis/python-nginx:chore_rust_install
# FROM quay.io/cdis/python-nginx:pybase3-1.4.2


ENV appname=indexd

RUN pip install --upgrade pip

RUN apk add --update \
    postgresql-libs postgresql-dev libffi-dev libressl-dev \
    linux-headers musl-dev gcc \
    curl bash git vim logrotate

# RUN rustc --version

# RUN apk update \
#     && apk add postgresql-libs postgresql-dev libffi-dev libressl-dev \
#     && apk add linux-headers musl-dev gcc \
#     && apk add curl bash git vim logrotate


RUN mkdir -p /var/www/$appname \
    && mkdir -p /var/www/.cache/Python-Eggs/ \
    && mkdir /run/nginx/ \
    && ln -sf /dev/stdout /var/log/nginx/access.log \
    && ln -sf /dev/stderr /var/log/nginx/error.log \
    && chown nginx -R /var/www/.cache/Python-Eggs/ \
    && chown nginx /var/www/$appname

EXPOSE 80

# install poetry
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python

COPY . /$appname
COPY ./deployment/uwsgi/uwsgi.ini /etc/uwsgi/uwsgi.ini
COPY ./deployment/uwsgi/wsgi.py /$appname/wsgi.py
COPY clear_prometheus_multiproc /$appname/clear_prometheus_multiproc
WORKDIR /$appname

# cache so that poetry install will run if these files change
COPY poetry.lock pyproject.toml /$appname/

# Run gen3authz from new branch
RUN git clone -q https://github.com/uc-cdis/gen3authz.git \
	&& cd gen3authz/ \
	&& git checkout fix/arborist_authrequest\
	&& cd python/ \
	&& source $HOME/.poetry/env \
	&& poetry config virtualenvs.create false \
	&& poetry install -vv --no-dev --no-interaction \
	&& poetry show -v

RUN pip install ./gen3authz/python

# install Indexd and dependencies via poetry
RUN source $HOME/.poetry/env \
    && poetry config virtualenvs.create false \
    && poetry install -vv --no-dev --no-interaction \
    && poetry show -v

RUN COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >$appname/index/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>$appname/index/version_data.py

WORKDIR /var/www/$appname

CMD /dockerrun.sh
