ARG BASE_VERSION=3.0.1
ARG REGISTRY=quay.io
ARG NAME=indexd

FROM ${REGISTRY}/ncigdc/python3.8-builder:${BASE_VERSION} as build
ARG NAME
ARG PIP_INDEX_URL
ENV PIP_INDEX_URL=$PIP_INDEX_URL
ARG REQUIREMENTS_GDC_LIBRARIES_FILE

WORKDIR /${NAME}
# Copy only requirements.txt here so Docker can cache the layer with
# the installed packages if the pins don't change.
COPY requirements.txt ./
RUN pip3 install --no-deps -r requirements.txt

# Now install the code for indexd itself.
COPY . .
RUN pip3 install --no-deps .


FROM ${REGISTRY}/ncigdc/python3.8-httpd:${BASE_VERSION}
ARG NAME

LABEL org.opencontainers.image.title=${NAME} \
      org.opencontainers.image.description="${NAME} container image" \
      org.opencontainers.image.source="https://github.com/NCI-GDC/${NAME}" \
      org.opencontainers.image.vendor="NCI GDC"

RUN dnf install -y libpq-15.0

RUN mkdir -p /var/www/${NAME}/ \
  && chmod 777 /var/www/${NAME}

COPY wsgi.py /var/www/${NAME}/
COPY bin/${NAME} /var/www/${NAME}/
COPY --from=build /venv/lib/python3.8/site-packages /venv/lib/python3.8/site-packages

# Make indexd CLI utilities available for, e.g., DB schema migration.
COPY --from=build /venv/bin/indexd /venv/bin
COPY --from=build /venv/bin/index_admin.py /venv/bin
COPY --from=build /venv/bin/migrate_index.py /venv/bin


WORKDIR /var/www/${NAME}
