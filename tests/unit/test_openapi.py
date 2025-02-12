import json

try:
    from importlib.resources import as_file, files
except ImportError:
    from importlib_resources import (  # type: ignore[import-not-found, no-redef]
        files,
        as_file,
    )

import openapi_spec_validator
from openapi_spec_validator import exceptions as specs_exceptions
from openapi_spec_validator import readers as specs_readers

import openapis


def test_valid_openapi():
    with as_file(files(openapis.__name__).joinpath("swagger.yaml")) as filename:
        spec, url = specs_readers.read_from_filename(filename)

        if not isinstance(spec, dict):
            raise specs_exceptions.OpenAPISpecValidatorError(
                "root node is not a mapping"
            )
        # ensure the spec is valid JSON
        spec = json.loads(json.dumps(spec))

        openapi_spec_validator.validate(spec, url)
