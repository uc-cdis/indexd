ARG AZLINUX_BASE_VERSION=master

# For local development
# FROM quay.io/cdis/python-nginx-al2:feat_python-nginx AS base

FROM 707767160287.dkr.ecr.us-east-1.amazonaws.com/gen3/python-nginx-al2:feat_python-nginx AS base

ENV appname=indexd

WORKDIR /${appname}

RUN chown -R gen3:gen3 /$appname

# Builder stage
FROM base AS builder

USER gen3

COPY poetry.lock pyproject.toml /${appname}/

RUN poetry install -vv --only main --no-interaction

COPY --chown=gen3:gen3 . /$appname
COPY --chown=gen3:gen3 ./deployment/wsgi/wsgi.py /$appname/wsgi.py

# Run poetry again so this app itself gets installed too
RUN poetry install --without dev --no-interaction

RUN git config --global --add safe.directory /${appname} && COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" > /$appname/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >> /$appname/version_data.py

# Final stage
FROM base

COPY --from=builder /$appname /$appname

# Switch to non-root user 'gen3' for the serving process
USER gen3

WORKDIR /$appname

CMD ["/bin/bash", "-c", "/indexd/dockerrun.bash"]
