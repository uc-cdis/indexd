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
        'cdispyutils',
        'indexclient',
        'doiclient',
    ],
    dependency_links=[
        "git+https://github.com/uc-cdis/cdis-python-utils.git@0.1.7#egg=cdispyutils",
        "git+https://github.com/uc-cdis/indexclient.git@a5ecad645751bc2a68465dd6ebb71cd063f8034f#egg=indexclient",
        "git+https://github.com/uc-cdis/doiclient.git@d28867a2916f5873b816ea1d6e2d5046d7865c64#egg=doiclient"
    ],
)
