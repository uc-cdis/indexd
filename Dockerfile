ARG AZLINUX_BASE_VERSION=master

FROM 707767160287.dkr.ecr.us-east-1.amazonaws.com/gen3/python-build-base:${AZLINUX_BASE_VERSION} as base
# FROM quay.io/cdis/python-build-base:${AZLINUX_BASE_VERSION} as base

ENV appname=indexd
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1

FROM base as builder

RUN source /venv/bin/activate

WORKDIR /$appname

COPY poetry.lock pyproject.toml /$appname/
RUN pip install --upgrade poetry \
    && poetry install --without dev --no-interaction

COPY . /$appname
COPY ./deployment/wsgi/wsgi.py /$appname/wsgi.py
RUN poetry install --without dev --no-interaction

RUN COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >$appname/index/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>$appname/index/version_data.py

FROM base

RUN source /venv/bin/activate

COPY --from=builder /venv /venv
COPY --from=builder /$appname /$appname

WORKDIR /$appname

ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8
CMD ["gunicorn", "-c", "deployment/wsgi/gunicorn.conf.py"]
