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
    classifiers=[
        "Framework :: Flask",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Internet :: WWW/HTTP :: HTTP Servers",
    ],
    python_requires=">=3.7",
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
            "jsonschema-spec>=0.1.6",  # used by openapi-spec-validator and have bug in 0.1.4
        ],
    },
    install_requires=[
        "flask>=2.2",
        "jsonschema>3",
        "sqlalchemy<1.4",  # TODO: Unpin sqlalchemy. Pinning is only required when psqlgraph is involved.
        "sqlalchemy-utils>=0.32",
        "psycopg2>=2.7",
        "cdislogging>=1.0",
        "requests",
        "ddtrace>=2.9.1",
        "importlib-metadata>=1.4; python_version < '3.8'",
        # jsonschema-spec 0.1.6 depends on typing-extensions<4.6.0
        "typing-extensions<4.6.0",
    ],
)
