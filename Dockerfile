FROM alpine:3.5
LABEL maintainer CDIS <cdissupport@opensciencedatacloud.org>

COPY . /indexd/
RUN apk --no-cache add \
        ca-certificates \
	gcc \
	git \
	musl-dev \
	nginx \
	postgresql-dev \
        python \
	python-dev \
        py2-pip \
	sudo \
	uwsgi \
	uwsgi-python \
	&& rm /etc/nginx/conf.d/default.conf \
	&& mkdir /run/nginx/ \
	&& { \
		echo 'server {'; \
		echo 'location / {'; \
		echo 'include uwsgi_params;'; \
		echo 'uwsgi_pass unix:///tmp/uwsgi.sock;'; \
		echo '}'; \
		echo '}'; \
	} >/etc/nginx/conf.d/nginx.conf \
	&& echo "daemon off;" >> /etc/nginx/nginx.conf \
	&& { \
		echo '[uwsgi]'; \
		echo 'socket = /tmp/uwsgi.sock'; \
		echo 'chown-socket = nginx:nginx'; \
		echo 'chmod-socket = 664'; \
		echo 'cheaper = 2'; \
		echo 'processes = 16'; \
		echo 'wsgi-file=/var/www/indexd/wsgi.py'; \
		echo 'plugins = python'; \
	} >/etc/uwsgi/uwsgi.ini \
        && cd /indexd \
        && python setup.py install \
        && mkdir -p /var/www/indexd/ \
        && chmod 777 /var/www/indexd \
        && cp /indexd/wsgi.py /var/www/indexd/wsgi.py \
        && cp /indexd/bin/indexd /var/www/indexd/indexd \
	&& ln -sf /dev/stdout /var/log/nginx/access.log \
	&& ln -sf /dev/stderr /var/log/nginx/error.log

EXPOSE 80

WORKDIR /var/www/indexd
CMD  /indexd/dockerrun.sh
