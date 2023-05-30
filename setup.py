from setuptools import find_packages, setup

setup(
    name="indexd",
    use_scm_version={
        "local_scheme": "dirty-tag",
        "write_to": "indexd/_version.py",
    },
    setup_requires=["setuptools_scm<6"],
    packages=find_packages(),
    package_data={
        "index": [
            "schemas/*",
        ]
    },
    scripts=["bin/index_admin.py", "bin/indexd", "bin/migrate_index.py"],
    install_requires=[
        "flask>=1.1",
        "jsonschema>3",
        "sqlalchemy<1.4",
        "sqlalchemy-utils>=0.32",
        "psycopg2>=2.7",
        "cdislogging>=1.0",
        "ddtrace",
        "dataclasses; python_version < '3.7'",
    ],
)
