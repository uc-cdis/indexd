import codecs
import json

import swagger_spec_validator.util
import yaml
from swagger_spec_validator.common import SwaggerValidationError


def test_valid_openapi():
    filename = "openapis/swagger.yaml"
    with codecs.open(filename, encoding="utf-8") as f:
        url = "file://" + filename + "#"
        spec = yaml.safe_load(f)
        if not isinstance(spec, dict):
            raise SwaggerValidationError("root node is not a mapping")
        # ensure the spec is valid JSON
        spec = json.loads(json.dumps(spec))
        validator = swagger_spec_validator.util.get_validator(spec, url)
        validator.validate_spec(spec, url)
