from setuptools import setup, find_packages

setup(
    name='indexd',
    version='2.3.0',
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
        'sqlalchemy-utils<1',
        'psycopg2>=2.7',
        'cdislogging',
        'indexclient',
        'doiclient',
        'dosclient',
        'future<1'
    ],
    dependency_links=[
        "git+https://github.com/uc-cdis/cdislogging.git@0.0.2#egg=cdislogging",
        "git+https://github.com/NCI-GDC/indexclient.git@1.5.10#egg=indexclient",
    ],
)
