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
        'flask',
        'jsonschema',
        'sqlalchemy',
        'sqlalchemy-utils',
        'future',
        'cdislogging',
        'indexclient',
        'doiclient',
        'dosclient',
    ],
)
