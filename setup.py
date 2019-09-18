from setuptools import setup, find_packages

setup(
    name="indexd",
    version="0.1",
    packages=find_packages(),
    package_data={"index": ["schemas/*"]},
    install_requires=[
        "flask==1.1.1",
        "jsonschema==2.5.1",
        "sqlalchemy==1.3.3",
        "sqlalchemy-utils>=0.33.11",
        "psycopg2>=2.7",
        "cdislogging>=0.0.2",
        "indexclient",
        "doiclient",
        "dosclient",
        "authutils",
        "gen3rbac",
    ],
    dependency_links=[
        "git+https://github.com/uc-cdis/cdislogging.git@0.0.2#egg=cdislogging",
        "git+https://github.com/uc-cdis/indexclient.git@1.6.0#egg=indexclient",
        "git+https://github.com/uc-cdis/doiclient.git@1.0.0#egg=doiclient",
        "git+https://github.com/uc-cdis/dosclient.git@1.0.0#egg=dosclient",
    ],
)
