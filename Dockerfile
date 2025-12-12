ARG AZLINUX_BASE_VERSION=3.13-pythonnginx

# Base stage with python-build-base
FROM quay.io/cdis/amazonlinux-base:${AZLINUX_BASE_VERSION} AS base

ENV appname=indexd

WORKDIR /${appname}

RUN chown -R gen3:gen3 /${appname}

# Builder stage
FROM base AS builder

USER gen3

# Copy poetry manifests for dependency caching
COPY poetry.lock pyproject.toml /${appname}/

# Remove the old poetry pipx package from base image, currently set to <2.0
RUN pipx uninstall poetry
# Install poetry version >=2.0 for export compatibility
RUN pip install --upgrade "poetry>=2.0.0,<3.0.0"

# Install the plugin for requirements export
RUN poetry self add poetry-plugin-export

# Install dependencies, not app files
RUN poetry install -vv --no-root --only main --no-interaction

COPY --chown=gen3:gen3 . /${appname}
COPY --chown=gen3:gen3 ./deployment/wsgi/wsgi.py /${appname}/wsgi.py

# Install app
RUN poetry install --without dev --no-interaction

# Export requirements for pip in final image
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes

RUN git config --global --add safe.directory ${appname} && COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" > ${appname}/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >> ${appname}/version_data.py

# Final stage
FROM base

USER root

COPY --from=builder /${appname} /${appname}
COPY --chown=gen3:gen3 ./dockerrun.bash /${appname}/dockerrun.bash

RUN dnf -y install vim

# Install dependencies
RUN pip install --no-cache-dir -r /${appname}/requirements.txt

# Switch to non-root user 'gen3' for the serving process
USER gen3

WORKDIR /${appname}

CMD ["/bin/bash", "-c", "/indexd/dockerrun.bash"]
