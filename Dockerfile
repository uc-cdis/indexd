ARG BASE_VERSION=3.2.1
ARG REGISTRY=docker.osdc.io
ARG SERVICE_NAME=indexd
ARG PYTHON_VERSION=python3.13

FROM ${REGISTRY}/ncigdc/${PYTHON_VERSION}-builder:${BASE_VERSION} AS build
ARG SERVICE_NAME
ARG PIP_INDEX_URL
ARG PYTHON_VERSION

# avoids used detach heads in computing versions in gitlab
ARG GIT_BRANCH_NAME
ENV CI_COMMIT_REF_NAME=$GIT_BRANCH_NAME

WORKDIR /${SERVICE_NAME}

COPY . .
RUN pip install --upgrade setuptools pip \
    && pip install versionista>=1.1.0 \
    && python3 -m setuptools_scm \
    && pip install --no-deps -r requirements.txt .

FROM ${REGISTRY}/ncigdc/${PYTHON_VERSION}-httpd:${BASE_VERSION}
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

RUN dnf install -y libpq-15.0 \
    && mkdir -p /var/www/${SERVICE_NAME}/ \
    && chmod 777 /var/www/${SERVICE_NAME}

COPY wsgi.py /var/www/${SERVICE_NAME}/
COPY .docker/indexd.conf /etc/httpd/conf.d/indexd.conf
COPY --from=build /venv/lib/${PYTHON_VERSION}/site-packages /venv/lib/${PYTHON_VERSION}/site-packages

# Make indexd CLI utilities available for, e.g., DB schema migration.
COPY --from=build /venv/bin/indexd /venv/bin
COPY --from=build /venv/bin/index_admin.py /venv/bin
COPY --from=build /venv/bin/migrate_index.py /venv/bin


WORKDIR /var/www/${SERVICE_NAME}
