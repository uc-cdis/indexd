from importlib.metadata import distribution

import logstick
from ddtrace import auto  # noqa

logstick.configure_logging(
    namespace=__name__, disable_existing_loggers=False, extra_namespaces=["werkzeug"]
)
__distribution = distribution(__name__)
VERSION = __distribution.version
