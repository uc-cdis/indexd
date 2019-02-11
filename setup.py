from setuptools import setup, find_packages

setup(
    name='indexd',
    version='0.1',
    packages=find_packages(),
    package_data={
        'index': [
            'schemas/*',
        ]
    },
    install_requires=[
        'flask==0.12.4',
        'jsonschema==2.5.1',
        'sqlalchemy==1.0.8',
        'sqlalchemy-utils>=0.32.21',
        'psycopg2>=2.7',
        'future>=0.16.0,<1.0.0',
        'cdislogging',
        'indexclient',
        'doiclient',
        'dosclient',
        'hsclient',
    ],
    dependency_links=[
        "git+https://github.com/uc-cdis/cdislogging.git@0.0.2#egg=cdislogging",
        "git+https://github.com/uc-cdis/indexclient.git@1.5.5#egg=indexclient",
        "git+https://github.com/uc-cdis/doiclient.git@1.0.0#egg=doiclient",
        "git+https://github.com/uc-cdis/dosclient.git@1.1.0#egg=dosclient",
        "git+https://github.com/uc-cdis/hsclient.git@1.0.0#egg=hsclient",
    ],
)
