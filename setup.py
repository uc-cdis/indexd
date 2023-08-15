from pathlib import Path

from setuptools import find_packages, setup

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="indexd",
    description="Data indexing and tracking service.",
    author="NCI GDC",
    author_email="gdc_dev_questions-aaaaae2lhsbell56tlvh3upgoq@cdis.slack.com",
    url="https://github.com/NCI-GDC/indexd",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    package_data={
        "index": [
            "schemas/*",
        ]
    },
    scripts=["bin/index_admin.py", "bin/indexd", "bin/migrate_index.py"],
    extras_require={
        "dev": [
            "pytest",
            "pytest-cov",
            "pytest-flask",
            "PyYAML",
            "openapi-spec-validator",
        ],
    },
    install_requires=[
        "flask>=1.1",
        "jsonschema>3",
        "sqlalchemy<1.4",
        "sqlalchemy-utils>=0.32",
        "psycopg2>=2.7",
        "cdislogging>=1.0",
        "requests",
        "ddtrace",
        "dataclasses; python_version < '3.7'",
        "importlib-metadata; python_version < '3.8'",
    ],
)
