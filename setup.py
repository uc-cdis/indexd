from setuptools import setup, find_packages

setup(
    name='indexd',
    version='2.2.0',
    packages=find_packages(),
    package_data={
        'index': [
            'schemas/*',
        ]
    },
    install_requires=[
        'flask==0.12.4',
        'jsonschema==2.5.1',
        'sqlalchemy==1.3.3',
        'sqlalchemy-utils>=0.32.21',
        'psycopg2>=2.7',
        'future>=0.16.0,<1.0.0',
        'cdislogging',
        'indexclient',
        'doiclient',
        'dosclient',
    ],
)
