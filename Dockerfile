# To run: docker run -v /path/to/wsgi.py:/var/www/indexd/wsgi.py --name=indexd -p 81:80 indexd
# To check running container: docker exec -it indexd /bin/bash

FROM python3.9-gpe-979

USER root

ENV appname=indexd

# This one is causing an issue
RUN pip3 install --upgrade poetry

RUN yum update -y && yum install -y \
    gcc gcc-c++ kernel-devel make libffi-devel libxml2-devel libxslt-devel postgresql-devel python3-devel \
    bash git vim


EXPOSE 80

WORKDIR /$appname

# copy ONLY poetry artifact, install the dependencies but not indexd
# this will make sure than the dependencies is cached
COPY poetry.lock pyproject.toml /$appname/
RUN poetry config \
    && poetry install -vv --no-root --no-dev --no-interaction \
    && poetry show -v \
    && poetry add gunicorn

# copy source code ONLY after installing dependencies
COPY . /$appname
COPY ./deployment/wsgi/wsgi.py /$appname/wsgi.py

# install indexd
RUN poetry config \
    && poetry install -vv --no-dev --no-interaction \
    && poetry show -v

RUN COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >$appname/index/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>$appname/index/version_data.py

# USER nobody
RUN poetry config \
    && poetry add gunicorn
CMD ["poetry run gunicorn -c deployment/uwsgi/gunicorn.conf.py"]
