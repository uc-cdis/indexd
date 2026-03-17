ARG AZLINUX_BASE_VERSION=3.13-pythonnginx

# Base stage with python-build-base
FROM quay.io/cdis/amazonlinux-base:${AZLINUX_BASE_VERSION} AS base

ENV appname=indexd

WORKDIR /${appname}

RUN chown -R gen3:gen3 /${appname}

# Builder stage
FROM base AS builder

USER root
RUN chown -R gen3:gen3 /venv

USER gen3

COPY poetry.lock pyproject.toml /${appname}/

# RUN python3 -m venv /env && . /env/bin/activate &&
RUN poetry install -vv --no-interaction --without dev

COPY --chown=gen3:gen3 . /${appname}

RUN poetry install -vv --no-interaction --without dev

RUN echo "Latest tags (sorted by date):" && git tag --contains HEAD --sort=-taggerdate
RUN echo "Git describe tags:" && git describe --always --tags
RUN VERSION=$(git tag --contains HEAD --sort=-taggerdate | head -n 1) && if [ -z "$VERSION" ]; then \
        echo "Tag command failed or returned empty. Falling back to git describe."; \
        VERSION=$(git describe --always --tags); \
        echo "Using git describe VERSION: $VERSION"; \
    else \
        echo "Successfully found tag, VERSION: $VERSION"; \
    fi && git config --global --add safe.directory ${appname} && COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" > ${appname}/version_data.py \
    && echo "VERSION=\"${VERSION}\"" >> ${appname}/version_data.py


# Final stage
FROM base

COPY --from=builder /${appname} /${appname}
COPY --from=builder /venv /venv
ENV  PATH="/usr/sbin:$PATH"
USER root
RUN mkdir -p /var/log/nginx
RUN chown -R gen3:gen3 /var/log/nginx

# Switch to non-root user 'gen3' for the serving process

USER gen3

CMD ["/bin/bash", "-c", "/${appname}/dockerrun.bash"]
