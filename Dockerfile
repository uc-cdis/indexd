# TEMPORARY: points at the base-images branch build (branch `feat/pythonuv`, where the workflow
# tags `$BRANCH_NAME-3.13-pythonuv`). Revert to `3.13-pythonuv` before
# merging, or main will build against a branch tag that eventually gets pruned.
ARG AZLINUX_BASE_VERSION=feat_pythonuv-3.13-pythonuv

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

COPY uv.lock pyproject.toml /${appname}/

ENV UV_PROJECT_ENVIRONMENT=/venv

RUN uv sync --frozen --no-dev --no-install-project

COPY --chown=gen3:gen3 . /${appname}

RUN uv sync --frozen --no-dev

RUN git config --global --add safe.directory ${appname} && COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" > ${appname}/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >> ${appname}/version_data.py


# Final stage
FROM base

COPY --from=builder /${appname} /${appname}
COPY --from=builder /venv /venv
ENV  PATH="/usr/sbin:$PATH"
USER root

# Switch to non-root user 'gen3' for the serving process

USER gen3

CMD ["/bin/bash", "-c", "/${appname}/dockerrun.bash"]
