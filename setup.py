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
        'sqlalchemy',
        'sqlalchemy-utils>=0.32.21',
        'psycopg2>=2.7',
        'future>=0.16.0,<1.0.0',
        'cdislogging',
        'indexclient',
        'doiclient',
        'dosclient',
    ],
    dependency_links=[
        "git+https://github.com/uc-cdis/cdislogging.git@0.0.2#egg=cdislogging",
        "git+https://github.com/NCI-GDC/indexclient.git@1.3.10#egg=indexclient",
        "git+https://github.com/uc-cdis/doiclient.git@1.0.0#egg=doiclient",
        "git+https://github.com/uc-cdis/dosclient.git@1.0.0#egg=dosclient",
    ],
)
