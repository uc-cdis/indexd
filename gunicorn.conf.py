from __future__ import annotations

import datetime
import logging
import os
import sys

import json_log_formatter
from ddtrace import tracer

# Based on the example from https://github.com/benoitc/gunicorn/blob/master/examples/example_config.py
#
# Server socket
#
#   bind - The socket to bind.
#
#       A string of the form: 'HOST', 'HOST:PORT', 'unix:PATH'.
#       An IP is a valid HOST.
#
#   backlog - The number of pending connections. This refers
#       to the number of clients that can be waiting to be
#       served. Exceeding this number results in the client
#       getting an error when attempting to connect. It should
#       only affect servers under significant load.
#
#       Must be a positive integer. Generally set in the 64-2048
#       range.
#

bind = "0.0.0.0:80"
backlog = os.getenv("GUNICORN_BACKLOG", 2048)

#
# Worker processes
#
#   workers - The number of worker processes that this server
#       should keep alive for handling requests.
#
#       A positive integer generally in the 2-4 x $(NUM_CORES)
#       range. You'll want to vary this a bit to find the best
#       for your particular application's work load.
#
#   worker_class - The type of workers to use. The default
#       sync class should handle most 'normal' types of work
#       loads. You'll want to read
#       http://docs.gunicorn.org/en/latest/design.html#choosing-a-worker-type
#       for information on when you might want to choose one
#       of the other worker classes.
#
#       A string referring to a Python path to a subclass of
#       gunicorn.workers.base.Worker. The default provided values
#       can be seen at
#       http://docs.gunicorn.org/en/latest/settings.html#worker-class
#
#   worker_connections - For the eventlet and gevent worker classes
#       this limits the maximum number of simultaneous clients that
#       a single process can handle.
#
#       A positive integer generally set to around 1000.
#
#   threads - The number of worker threads for handling requests.
#       Run each worker with the specified number of threads.
#
#       A positive integer generally in the 2-4 x $(NUM_CORES) range.
#       You'll want to vary this a bit to find the best for your particular application's work load.
#       This setting only affects the Gthread worker type.
#
#   timeout - If a worker does not notify the master process in this
#       number of seconds it is killed and a new worker is spawned
#       to replace it.
#
#       Generally set to thirty seconds. Only set this noticeably
#       higher if you're sure of the repercussions for sync workers.
#       For the non sync workers it just means that the worker
#       process is still communicating and is not tied to the length
#       of time required to handle a single request.
#
#   keepalive - The number of seconds to wait for the next request
#       on a Keep-Alive HTTP connection.
#
#       A positive integer. Generally set in the 1-5 seconds range.
#

workers = os.getenv("GUNICORN_WORKERS", 3)
worker_class = os.getenv("GUNICORN_WORKER_CLASS", "sync")
worker_connections = os.getenv("GUNICORN_WORKER_CONNECTIONS", 2048)
threads = os.getenv("GUNICORN_THREADS", 1)
timeout = os.getenv("GUNICORN_TIMEOUT", 600)
graceful_timeout = os.getenv("GUNICORN_GRACEFUL_TIMEOUT", 600)
keepalive = os.getenv("GUNICORN_KEEPALIVE", 5)

max_requests = os.getenv("GUNICORN_MAX_REQUESTS", 0)
max_requests_jitter = os.getenv("GUNICORN_MAX_REQUESTS_JITTER", 0)

#
# Server mechanics
#
#   daemon - Detach the main Gunicorn process from the controlling
#       terminal with a standard fork/fork sequence.
#
#       True or False
#
#   raw_env - Pass environment variables to the execution environment.
#
#   pidfile - The path to a pid file to write
#
#       A path string or None to not write a pid file.
#
#   user - Switch worker processes to run as this user.
#
#       A valid user id (as an integer) or the name of a user that
#       can be retrieved with a call to pwd.getpwnam(value) or None
#       to not change the worker process user.
#
#   group - Switch worker process to run as this group.
#
#       A valid group id (as an integer) or the name of a user that
#       can be retrieved with a call to pwd.getgrnam(value) or None
#       to change the worker processes group.
#
#   umask - A mask for file permissions written by Gunicorn. Note that
#       this affects unix socket permissions.
#
#       A valid value for the os.umask(mode) call or a string
#       compatible with int(value, 0) (0 means Python guesses
#       the base, so values like "0", "0xFF", "0022" are valid
#       for decimal, hex, and octal representations)
#
#   tmp_upload_dir - A directory to store temporary request data when
#       requests are read. This will most likely be disappearing soon.
#
#       A path to a directory where the process owner can write. Or
#       None to signal that Python should choose one on its own.
#

daemon = False
raw_env = []
pidfile = None
umask = 0o27
user = "app"
group = "app"
tmp_upload_dir = None
worker_tmp_dir = "/dev/shm"

#
#   Logging
#
#   logfile - The path to a log file to write to.
#
#       A path string. "-" means log to stdout.
#
#   loglevel - The granularity of log output
#
#       A string of "debug", "info", "warning", "error", "critical"
#
errorlog = "-"
loglevel = "info"
accesslog = "-"


# Testing example from https://til.codeinthehole.com/posts/how-to-get-gunicorn-to-log-as-json/
class JsonRequestFormatter(json_log_formatter.JSONFormatter):
    def json_record(
        self,
        message: str,
        extra: dict[str, str | int | float],
        record: logging.LogRecord,
    ) -> dict[str, str | int | float]:
        # Convert the log record to a JSON object.
        # See https://docs.gunicorn.org/en/stable/settings.html#access-log-format

        response_time = datetime.datetime.strptime(
            record.args["t"], "[%d/%b/%Y:%H:%M:%S %z]"
        )
        url = record.args["U"]
        if record.args["q"]:
            url += f"?{record.args['q']}"

        span = tracer.current_span()
        trace_id, span_id = (
            (str((1 << 64) - 1 & span.trace_id), span.span_id) if span else (None, None)
        )

        return dict(
            ts=response_time.isoformat(),
            path=url,
            query=record.args["q"],
            http=dict(
                status_code=str(record.args["s"]),
                method=record.args["m"],
                response_body_bytes=record.args["b"],
                user_agent=record.args["a"],
                referer=record.args["f"],
                x_forwarded_for=record.args["{x-forwarded-for}i"],
            ),
            remote_addr=record.args["h"],
            remote_user=record.args["u"],
            protocol=record.args["H"],
            duration_in_ms=record.args["M"],
            traceparent=record.args["{traceparent}i"],
            tracestate=record.args["{tracestate}i"],
            dd=dict(
                trace_id=str(trace_id or 0),
                span_id=str(span_id or 0),
            ),
        )


class JsonErrorFormatter(json_log_formatter.JSONFormatter):
    def json_record(
        self,
        message: str,
        extra: dict[str, str | int | float],
        record: logging.LogRecord,
    ) -> dict[str, str | int | float]:
        payload: dict[str, str | int | float] = super().json_record(
            message, extra, record
        )
        span = tracer.current_span()
        trace_id, span_id = (
            (str((1 << 64) - 1 & span.trace_id), span.span_id) if span else (None, None)
        )
        payload["dd.trace_id"] = str(trace_id or 0)
        payload["dd.span_id"] = str(span_id or 0)
        payload["level"] = record.levelname
        return payload


gunicorn_loglevel = os.getenv("APP_GUNICORN_LOGLEVEL", "INFO")
generic_loglevel = os.getenv("APP_GENERIC_LOGLEVEL", "INFO")
# Ensure the two named loggers that Gunicorn uses are configured to use a custom
# JSON formatter.
logconfig_dict = {
    "version": 1,
    "formatters": {
        "json_request": {
            "()": JsonRequestFormatter,
        },
        "json_error": {
            "()": JsonErrorFormatter,
        },
    },
    "handlers": {
        "json_request": {
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
            "formatter": "json_request",
        },
        "json_error": {
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
            "formatter": "json_error",
        },
    },
    "root": {"level": "INFO", "handlers": []},
    "loggers": {
        "gunicorn.access": {
            "level": gunicorn_loglevel,
            "handlers": ["json_request"],
            "propagate": False,
        },
        "gunicorn.error": {
            "level": gunicorn_loglevel,
            "handlers": ["json_error"],
            "propagate": False,
        },
        "": {
            "level": generic_loglevel,
            "handlers": ["json_error"],
            "propagate": False,
        },
    },
}
