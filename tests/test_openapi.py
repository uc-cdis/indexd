import json
import pkg_resources

import openapi_spec_validator
from openapi_spec_validator import exceptions as specs_exceptions
from openapi_spec_validator import readers as specs_readers

import openapis


def test_valid_openapi():
    filename = pkg_resources.resource_filename(openapis.__name__, "swagger.yaml")
    spec, url = specs_readers.read_from_filename(filename)

    if not isinstance(spec, dict):
        raise specs_exceptions.OpenAPIValidationError("root node is not a mapping")
    # ensure the spec is valid JSON
    spec = json.loads(json.dumps(spec))

    openapi_spec_validator.validate_spec(spec, url)
