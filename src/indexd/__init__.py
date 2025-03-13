from importlib.metadata import distribution

import logstick

logstick.configure_logging(namespace=__name__, disable_existing_loggers=False)
__distribution = distribution(__name__)
VERSION = __distribution.version
