ARG base_version=1.2.0
ARG registry=quay.io

FROM ${registry}/ncigdc/python38-builder:${base_version} as build

# Copy only requirements.txt here so Docker can cache the layer with
# the installed packages if the pins don't change.
COPY requirements.txt /indexd/
WORKDIR /indexd
RUN pip3 install --no-deps -r requirements.txt

# Now install the code for indexd itself.
COPY . /indexd
RUN pip3 install --no-deps .


FROM ${registry}/ncigdc/python38-httpd:${base_version}
ARG version="Invalid-version"

LABEL org.label-schema.name="indexd" \
      org.label-schema.description="indexd container image" \
      org.label-schema.version=${version} \
      org.label-schema.schema-version="1.0"

RUN mkdir -p /var/www/indexd/ \
  && chmod 777 /var/www/indexd \
  && a2dissite 000-default

COPY wsgi.py /var/www/indexd/
COPY bin/indexd /var/www/indexd/
COPY --from=build /usr/local/lib/python3.8/dist-packages /usr/local/lib/python3.8/dist-packages

# Make indexd CLI utilities available for, e.g., DB schema migration.
COPY --from=build /usr/local/bin/*index* /usr/local/bin/

RUN ln -sf /dev/stdout /var/log/apache2/access.log \
  && ln -sf /dev/stdout /var/log/apache2/other_vhosts_access.log\
  && ln -sf /dev/stderr /var/log/apache2/error.log

WORKDIR /var/www/indexd
