# syntax=docker/dockerfile:1
ARG BASE_VERSION=3.0.9
ARG REGISTRY=docker.osdc.io
ARG SERVICE_NAME=indexd
ARG PYTHON_VERSION=python3.8

FROM ${REGISTRY}/ncigdc/${PYTHON_VERSION}-builder:${BASE_VERSION} AS build
ARG SERVICE_NAME
ARG PIP_INDEX_URL
ARG PYTHON_VERSION

WORKDIR /${SERVICE_NAME}

COPY . .
RUN pip install --upgrade setuptools pip \
    && pip install versionista>=1.1.0 --extra-index-url https://nexus.osdc.io/repository/pypi-gdc-releases/simple \
    && python3 -m setuptools_scm \
    && pip install --no-deps -r requirements.txt .

FROM ${REGISTRY}/ncigdc/${PYTHON_VERSION}:${BASE_VERSION}
ARG NAME
ARG PYTHON_VERSION
ARG SERVICE_NAME
ARG GIT_BRANCH
ARG COMMIT
ARG BUILD_DATE

LABEL org.opencontainers.image.title="${SERVICE_NAME}" \
      org.opencontainers.image.description="${SERVICE_NAME} container image" \
      org.opencontainers.image.source="https://github.com/NCI-GDC/${SERVICE_NAME}" \
      org.opencontainers.image.vendor="NCI GDC" \
      org.opencontainers.image.ref.name="${SERVICE_NAME}:${GIT_BRANCH}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}"

RUN dnf install -y libpq-15.0 && \
    mkdir -p /var/www/${SERVICE_NAME}/ && \
    chmod 777 /var/www/${SERVICE_NAME}

COPY wsgi.py /var/www/${SERVICE_NAME}/wsgi.py
COPY gunicorn.conf.py /var/www/${SERVICE_NAME}/gunicorn.conf.py
COPY --from=build /venv/lib/${PYTHON_VERSION}/site-packages /venv/lib/${PYTHON_VERSION}/site-packages

# Make indexd CLI utilities available for, e.g., DB schema migration.
COPY --from=build /venv/bin/indexd /venv/bin
COPY --from=build /venv/bin /venv/bin
COPY --from=build /venv/bin/index_admin.py /venv/bin
COPY --from=build /venv/bin/migrate_index.py /venv/bin


WORKDIR /var/www/${SERVICE_NAME}
EXPOSE 80 80
CMD [ "/venv/bin/gunicorn", \
      "wsgi" ]

RUN chown -R app:app /venv /var/www/${SERVICE_NAME}

USER app:app
