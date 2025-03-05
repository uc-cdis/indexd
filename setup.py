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
        "Programming Language :: Python :: 3.8",
        "Topic :: Internet :: WWW/HTTP :: HTTP Servers",
    ],
    python_requires=">=3.8",
    packages=find_packages(),
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
        "flask>=2.2",
        "jsonschema>4.17",
        "importlib_resources<6.2",  # >=6.2 fails for py38 but works ok for 39+
        "sqlalchemy<1.4",  # TODO: Unpin sqlalchemy. Pinning is only required when psqlgraph is involved.
        "sqlalchemy-utils>=0.32",
        "psycopg2>=2.7",
        "cdislogging>=1.0",
        "requests>=2.32.2",
        "ddtrace~=3.0",
        "importlib-metadata>=1.4",
        "typing-extensions",
        "zipp>=3.19.1",
        "werkzeug>=3.0.6",
        "gunicorn>=23.0.0",
        "setproctitle>=1.3.4",
        "JSON-log-formatter>=1.1",
    ],
)
