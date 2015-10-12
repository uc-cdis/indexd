from setuptools import setup

setup(
    name='indexd',
    version='0.1',
    packages=[
        'indexd',
        'indexd.index',
        'indexd.alias',
    ],
    package_data={
        'index': [
            'schemas/*',
        ]
    },
    install_requires=[
        'flask==0.10.1',
        'requests==2.7.0',
        'jsonschema==2.5.1',
    ],
)
