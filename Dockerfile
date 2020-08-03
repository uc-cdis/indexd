ARG base_version=1.0.1
ARG registry=quay.io

FROM ${registry}/ncigdc/python35-builder:${base_version} as build

# Copy only requirements.txt here so Docker can cache the layer with
# the installed packages if the pins don't change.
COPY requirements.txt /indexd/
WORKDIR /indexd
RUN pip3 install --no-deps -r requirements.txt

# Now install the code for indexd itself.
COPY . /indexd
RUN pip3 install --no-deps .


FROM ${registry}/ncigdc/python35-httpd:${base_version}

LABEL org.label-schema.name="indexd" \
      org.label-schema.description="indexd container image" \
      org.label-schema.version="2.4.0" \
      org.label-schema.schema-version="1.0"

RUN mkdir -p /var/www/indexd/ \
  && chmod 777 /var/www/indexd \
  && a2dissite 000-default

COPY wsgi.py /var/www/indexd/ 
COPY bin/indexd /var/www/indexd/ 
COPY --from=build /usr/local/lib/python3.5/dist-packages /usr/local/lib/python3.5/dist-packages

# Make indexd CLI utilities available for, e.g., DB schema migration.
COPY --from=build /usr/local/bin/*index* /usr/local/bin/

RUN ln -sf /dev/stdout /logdir/access.log \
  && ln -sf /dev/stdout /logdir/other_vhosts_access.log\
  && ln -sf /dev/stderr /logdir/error.log

WORKDIR /var/www/indexd
