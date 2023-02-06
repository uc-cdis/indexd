from setuptools import setup, find_packages

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
        "flask~=1.1",
        "jsonschema>3,<4",
        "sqlalchemy~=1.3",
        # Support Python 2 until everything that uses indexd in its tests has been updated.
        "sqlalchemy-utils>=0.32,<0.36.4",
        "psycopg2~=2.7",
        "cdislogging~=1.0",
        "doiclient @ git+https://github.com/uc-cdis/doiclient.git@1.0.0#egg=doiclient",
        "dosclient @ git+https://github.com/uc-cdis/dosclient.git@1.0.0#egg=dosclient",
        "future~=0.18",
        "Werkzeug~=0.16",
        "ddtrace~=1.6.0",
    ],
)
