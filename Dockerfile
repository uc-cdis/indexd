FROM quay.io/ncigdc/apache-base:1.0.3-py3.5 as build


COPY . /indexd
WORKDIR /indexd

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    python3 \
    python3-dev \
    python3-pip \
    python3-setuptools \
    libpq-dev \
    libpq5 \
    gcc \
 && pip3 install wheel \
 && pip3 install -r build/requirements.txt \
 && python3 setup.py install

FROM quay.io/ncigdc/apache-base:1.0.3-py3.5

LABEL org.label-schema.name="indexd" \
      org.label-schema.description="indexd container image" \
      org.label-schema.version="2.2.0" \
      org.label-schema.schema-version="1.0"

RUN mkdir -p /var/www/indexd/ \
    && chmod 777 /var/www/indexd \
    && a2dissite 000-default \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
       libpq5  

COPY wsgi.py /var/www/indexd/ 
COPY bin/indexd /var/www/indexd/ 
COPY --from=build /usr/local/lib/python3.5/dist-packages /usr/local/lib/python3.5/dist-packages
COPY . /indexd

WORKDIR /var/www/indexd


