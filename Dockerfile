# syntax=docker.osdc.io/docker/dockerfile:1
ARG BASE_VERSION=3.5.0
ARG REGISTRY=docker.osdc.io
ARG SERVICE_NAME=indexd
ARG PYTHON_VERSION=python3.13

FROM ${REGISTRY}/ncigdc/${PYTHON_VERSION}-builder:${BASE_VERSION} AS build
ARG PIP_INDEX_URL=https://nexus.osdc.io/repository/pypi-gdc-releases/simple
ARG PYTHON_VERSION
ARG SERVICE_NAME

# avoids used detach heads in computing versions in gitlab
ARG GIT_BRANCH_NAME
ENV CI_COMMIT_REF_NAME=$GIT_BRANCH_NAME \
    PIP_INDEX_URL=$PIP_INDEX_URL

# When looking for its virtual environment, uv will not respect VIRTUAL_ENV. It expects its
# virtualenv to be in a `.venv` subdirectory, even if we provide an explicit path to `uv venv`.
# This environment variable overrides where uv expects its virtual environment to be.
ENV UV_PROJECT_ENVIRONMENT='/venv'

WORKDIR /${SERVICE_NAME}

COPY . .
RUN uv sync --locked --no-dev --no-editable

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

ENV DD_GIT_COMMIT_SHA=${COMMIT} \
    DD_GIT_REPOSITORY_URL=https://github.com/NCI-GDC/${SERVICE_NAME}

RUN dnf install -y libpq-17.4 && \
    mkdir -p /var/www/${SERVICE_NAME}/ && \
    chmod 777 /var/www/${SERVICE_NAME}

COPY --chown=app:app wsgi.py /var/www/${SERVICE_NAME}/wsgi.py
COPY --chown=app:app gunicorn.conf.py /var/www/${SERVICE_NAME}/gunicorn.conf.py
COPY --chown=app:app --from=build /venv /venv


WORKDIR /var/www/${SERVICE_NAME}
EXPOSE 80 443
USER app:app
CMD [ "ddtrace-run", \
      "/venv/bin/gunicorn", \
      "wsgi" ]
