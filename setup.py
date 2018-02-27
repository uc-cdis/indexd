from setuptools import setup

setup(
    name='indexd',
    version='0.1',
    packages=[
        'indexd',
        'indexd.auth',
        'indexd.auth.drivers',
        'indexd.index',
        'indexd.index.drivers',
        'indexd.alias',
        'indexd.alias.drivers',
    ],
    package_data={
        'index': [
            'schemas/*',
        ]
    },
    install_requires=[
        'flask==0.10.1',
        'jsonschema==2.5.1',
        'sqlalchemy==1.0.8',
        'sqlalchemy-utils>=0.32.21',
    	'psycopg2>=2.7',
        'cdislogging',
        'indexclient',
        'doiclient',
    ],
    dependency_links=[
        "git+https://github.com/uc-cdis/cdislogging.git@0.0.2#egg=cdislogging",
        "git+https://github.com/uc-cdis/indexclient.git@1.3.1#egg=indexclient",
        "git+https://github.com/uc-cdis/doiclient.git@77d0a0b4d1ba4ea62e172c174a587a1851a686f9#egg=doiclient"
    ],
)
