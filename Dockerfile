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
