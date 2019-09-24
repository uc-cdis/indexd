FROM quay.io/ncigdc/apache-base:1.0.3 as build


COPY . /indexd
WORKDIR /indexd

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    python2.7 \
    python-dev \
    python-pip \
    python-setuptools \
    libpq-dev \
    libpq5 \
    gcc \
 && pip install wheel \
 && pip install -r build/requirements.txt \
 && python setup.py install 

FROM quay.io/ncigdc/apache-base:1.0.3

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
COPY --from=build /usr/local/lib/python2.7/dist-packages /usr/local/lib/python2.7/dist-packages

WORKDIR /var/www/indexd


