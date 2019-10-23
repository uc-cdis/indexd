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
        'flask<2',
        'jsonschema<3',
        'sqlalchemy==1.3.3',
        'cdislogging<2',
        'indexclient<2',
        'doiclient<2',
        'dosclient<2',
        'future<1',
    ],
)
