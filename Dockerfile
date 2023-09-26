# To run: docker run -v /path/to/wsgi.py:/var/www/indexd/wsgi.py --name=indexd -p 81:80 indexd
# To check running container: docker exec -it indexd /bin/bash

FROM quay.io/cdis/amazonlinux:python3.9-master as build-deps

USER root

ENV appname=indexd

RUN pip3 install --upgrade poetry

RUN yum update -y && yum install -y --setopt install_weak_deps=0 \
    kernel-devel libffi-devel libxml2-devel libxslt-devel postgresql-devel python3-devel \
    git

WORKDIR /$appname

# copy ONLY poetry artifact, install the dependencies but not indexd
# this will make sure that the dependencies is cached
COPY poetry.lock pyproject.toml /$appname/
RUN poetry config virtualenvs.in-project true \
    && poetry install -vv --no-root --only main --no-interaction \
    && poetry show -v

# copy source code ONLY after installing dependencies
COPY . /$appname
COPY ./deployment/wsgi/wsgi.py /$appname/wsgi.py

# install indexd
RUN poetry config virtualenvs.in-project true \
    && poetry install -vv --no-dev --no-interaction \
    && poetry show -v

RUN COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >$appname/index/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>$appname/index/version_data.py

#Creating the runtime image
FROM quay.io/cdis/amazonlinux:python3.9-feat_GPE-979

ENV appname=indexd

USER root

EXPOSE 80

RUN pip3 install --upgrade poetry

RUN yum update -y && yum install -y --setopt install_weak_deps=0 \
    postgresql-devel shadow-utils\
    bash

RUN useradd -ms /bin/bash appuser

COPY --from=build-deps --chown=appuser:appuser /$appname /$appname

WORKDIR /$appname

USER appuser

RUN poetry add gunicorn

CMD ["poetry", "run", "gunicorn", "-c", "deployment/wsgi/gunicorn.conf.py"]
