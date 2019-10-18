import json
import pkg_resources

import swagger_spec_validator.util
import yaml
from swagger_spec_validator.common import SwaggerValidationError

import openapis


def test_valid_openapi():
    filename = 'swagger.yaml'
    with pkg_resources.resource_stream(openapis.__name__, filename) as f:
        url = 'file:/' + f.name + '#'
        spec = yaml.safe_load(f)
        if not isinstance(spec, dict):
            raise SwaggerValidationError('root node is not a mapping')
        # ensure the spec is valid JSON
        spec = json.loads(json.dumps(spec))
        validator = swagger_spec_validator.util.get_validator(spec, url)
        validator.validate_spec(spec, url)
