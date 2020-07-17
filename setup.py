from setuptools import setup, find_packages

setup(
    name='indexd',
    version='2.4.0',
    packages=find_packages(),
    package_data={
        'index': [
            'schemas/*',
        ]
    },
    install_requires=[
        'flask~=1.1',
        'jsonschema~=2.5',
        'sqlalchemy~=1.3',
        # Support Python 2 until everything that uses indexd in its tests has been updated.
        'sqlalchemy-utils>=0.32,<0.36.4',
        'psycopg2~=2.7',
        'cdislogging @ git+https://github.com/uc-cdis/cdislogging.git@0.0.2#egg=cdislogging',
        'indexclient @ git+https://github.com/NCI-GDC/indexclient.git@2.0.0#egg=indexclient',
        'doiclient @ git+https://github.com/uc-cdis/doiclient.git@1.0.0#egg=doiclient',
        'dosclient @ git+https://github.com/uc-cdis/dosclient.git@1.0.0#egg=dosclient',
        'future~=0.18',
        'Werkzeug~=0.16',
    ],
)
