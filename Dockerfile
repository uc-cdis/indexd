ARG AZLINUX_BASE_VERSION=3.13-pythonnginx

FROM quay.io/cdis/amazonlinux-base:${AZLINUX_BASE_VERSION} AS builder
USER root
RUN dnf install -y --nodocs --setopt=install_weak_deps=False \
        gcc rust cargo openssl-devel libffi-devel libpq-devel pkgconfig \
    && dnf clean all \
    && /venv/bin/python -m pip uninstall -y pipx argcomplete userpath filelock platformdirs \
    && chown -R gen3:gen3 /venv
USER gen3
WORKDIR /indexd

COPY --chown=gen3:gen3 poetry.lock pyproject.toml ./
RUN POETRY_INSTALLER_NO_BINARY=cryptography,psycopg2-binary \
    poetry install --without dev --no-interaction --no-root

COPY --chown=gen3:gen3 . /indexd
RUN poetry install --without dev --no-interaction \
    && COMMIT=$(git rev-parse HEAD) \
    && VERSION=$(git describe --always --tags) \
    && printf 'COMMIT="%s"\nVERSION="%s"\n' "$COMMIT" "$VERSION" > indexd/version_data.py \
    && rm -rf /indexd/.git

FROM quay.io/cdis/amazonlinux-base:${AZLINUX_BASE_VERSION} AS runtime
USER root
RUN dnf install -y --nodocs --setopt=install_weak_deps=False libpq \
    && dnf update -y openssl openssl-libs \
    && dnf swap -y --setopt=install_weak_deps=False openssl-fips-provider-latest openssl-fips-provider-certified \
    && dnf swap -y --setopt=install_weak_deps=False coreutils-single coreutils \
    && grep -q '^error_log /var/log/nginx/error.log notice;$' /etc/nginx/nginx.conf \
    && grep -q '^    access_log /var/log/nginx/access.log main if=\$loggable;$' /etc/nginx/nginx.conf \
    && sed -i -e 's|^error_log /var/log/nginx/error.log notice;$|error_log /dev/stderr notice;|' \
              -e 's|^    access_log /var/log/nginx/access.log main if=\$loggable;$|    access_log /dev/stdout main if=$loggable;|' \
        /etc/nginx/nginx.conf \
    && dnf clean all \
    && dnf remove -y --setopt=protected_packages= --setopt=clean_requirements_on_remove=False \
        dnf yum python3-dnf git git-core python3.13-devel \
    && rpm -e --nodeps python3 $(rpm -qa 'python3-*' 'perl*') \
        python3.13-pip python3.13-pip-wheel curl-minimal rpm \
    && rm -rf /var/cache/dnf /var/cache/yum /var/tmp/dnf* /venv

COPY --from=builder --chown=0:0 /venv /venv
COPY --from=builder --chown=0:0 /indexd /indexd
RUN ldd "$(/venv/bin/python -c 'from cryptography.hazmat.bindings import _rust; print(_rust.__file__)')" | grep -q 'libcrypto.so' \
    && ldd "$(/venv/bin/python -c 'import psycopg2._psycopg as p; print(p.__file__)')" | grep -q 'libpq.so' \
    && /venv/bin/python -c 'import ssl; from cryptography.hazmat.bindings.openssl.binding import Binding; b = Binding(); assert b.ffi.string(b.lib.OpenSSL_version(0)).decode() == ssl.OPENSSL_VERSION, "cryptography and Python use different OpenSSL versions"' \
    && rm -rf /venv/lib/python3.13/site-packages/pip \
        /venv/lib/python3.13/site-packages/pip-*.dist-info \
    && find /venv/bin -mindepth 1 -maxdepth 1 ! -name 'python*' ! -name gunicorn -delete \
    && chmod -R go-w /venv /indexd \
    && install -o gen3 -g gen3 -m 600 /dev/null /indexd/bin/creds.json \
    && mkdir -p /var/log/nginx /var/tmp/prometheus_metrics \
    && chown gen3:gen3 /var/log/nginx /var/tmp/prometheus_metrics \
    && rm -rf /home/gen3/.cache /home/gen3/.local /home/gen3/.bash* \
        /usr/include/python3.13* /usr/lib64/python3.9 /usr/lib/python3.9 \
        /usr/lib64/python3.13/ensurepip /usr/share/python-wheels \
        /usr/share/python3-wheels /usr/share/python3.13-wheels \
        /usr/lib/rpm /usr/share/git-core /usr/lib64/security/pam_filter/upperLOWER \
        /usr/libexec /usr/local/sbin /usr/lib/systemd /etc/X11 /etc/update-motd.d \
        /etc/security/namespace.init /var/spool/mail/gen3 \
    && chown root:root /home/gen3 \
    && chmod 755 /home/gen3 \
    && find /usr/sbin -mindepth 1 -maxdepth 1 ! -name nginx -delete \
    && find /usr/bin -mindepth 1 -maxdepth 1 \
        ! -name python3.13 ! -name bash ! -name sh ! -name env \
        ! -name mkdir ! -name find ! -name rm -delete \
    && find /usr/local/bin -mindepth 1 -maxdepth 1 ! -name prometheus-entrypoint.sh -delete

FROM scratch
COPY --from=runtime / /

ENV PATH="/venv/bin:/usr/sbin:/usr/bin" \
    VIRTUAL_ENV=/venv \
    HOME=/home/gen3 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONIOENCODING=UTF-8 \
    PROMETHEUS_MULTIPROC_DIR=/var/tmp/prometheus_metrics
WORKDIR /indexd
USER gen3
ENTRYPOINT ["/usr/local/bin/prometheus-entrypoint.sh"]
CMD ["/bin/bash", "-c", "/indexd/dockerrun.bash"]
