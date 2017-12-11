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
    	'psycopg2==2.7.3.2',
        'cdispyutils',
    ],
    dependency_links=[
        "git+https://github.com/uc-cdis/cdis-python-utils.git@0.1.0#egg=cdispyutils",
    ],
)
