# Set up a local development environment

This guide will cover setting up an indexd development environment.

## Set up Working Directory

Clone the repo locally.

```console
git clone https://github.com/uc-cdis/indexd.git
```

Navigate to the cloned repository directory.

## Set up Python 3

The environment was tested with python 3.8 on WSL1.  You can use `bash` to install python 3 if it's not already available.

```console
sudo apt-get update
sudo apt-get install python3
```

### Set up a Virtual Environment

Set up a virtual environment for use with this project using `bash`:

```console
python3 -m venv py38-venv
. py38-venv/bin/activate
```

### Install Poetry

You can install Poetry.  Make sure the virtual environment is activated.

```console
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python
source $HOME/.poetry/env
```

You can install python dependencies using Poetry:

```console
poetry install -vv --no-interaction && poetry show -v
```

## Set up local Postgresql DB for testing

You can use a local postgresql for testing purposes.

### Set up local Postgresql DB on WSL

You can use `bash` to install postgres:

```console
sudo apt install postgresql-client-common
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get install postgresql-12
```

Make sure the cluster is started:

```console
sudo pg_ctlcluster 12 main start
```

### Set up local Postgresql DB on Mac

If you're on mac, you can install postgres using brew:

```console
brew install postgres
```

You may also need to create a user.

```console
/usr/local/opt/postgres/bin/createuser -s postgres
```

### Set up DB and users for testing

You'll need to connect to the postgresql and add test users and databases.

#### Connect to Postgresql on WSL

Connect to the local postgresql server

```console
sudo -i -u postgres
psql
```

#### Connect to Postgresql on Mac

If you're on a mac, use the following to connect to postgres:

```console
brew services start postgres
psql postgres
```

#### Helpful psql commands

It may be helpful to understand some psql commands too:

```console
\conninfo # check connection info
\l # list databases
\d # list tables in database
\c # list short connection info
\c postgres # connect to a database named postgres
\q # quit
```

#### Set up users in psql

Initialize a user within the psql console:

```console
CREATE USER indexduser WITH PASSWORD 'mypassword';
ALTER USER indexduser WITH PASSWORD 'indexdpassword';
\du
```

You can also view the local [PostgreSQL init.sql](./deployment/scripts/postgresql/postgresql_init.sql) and run these inside the local PostgreSQL server.

## Installation

The implementation for Indexd utilizes the Flask web framework and (by default) a SQLite3 database. This provides a minimum list of requirements and allows for deployment on a wide range of systems with next to no configuration overhead. That said, it is highly recommended to use pip and a virtualenv to isolate the installation.

Prior to installation, you will need to have postgresql installed.

On Mac
```bash
brew install postgresql
/usr/local/opt/postgres/bin/createuser -s postgres
```
On Linux
```bash
sudo apt-get install python-psycopg2
```

To install the implementation, assure you have poetry installed and simply run:

```bash
poetry install
```

To see how the automated tests (run in Travis CI) install Indexd, check out the `.travis.yml` file in the root directory of this repository.

## Installation with Docker

```bash
docker build --build-arg https_proxy=http://cloud-proxy:3128 --build-arg http_proxy=http://cloud-proxy:3128 -t indexd .

docker run -d --name=indexd -p 80:80 Indexd
docker exec indexd python /indexd/bin/index_admin.py create --username $username --password $password
docker exec indexd python /indexd/bin/index_admin.py delete --username $username
```

To run docker with an alternative settings file:

```
docker run -d -v local_settings.py:/var/www/indexd/local_settings.py --name=Indexd -p 80:80 indexd
```

## Configuration

There is a `/indexd/default_settings.py` file which houses, you guessed it, default configuration settings. If you want to provide an alternative configuration to override these, you must supply a `local_settings.py` in the same directory as the default settings. It must contain all the same configurations from the `default_settings.py`, though may have different values.

This works because on app startup, Indexd will attempt to include a `local_settings` python module (the attempted import happens in the `/indexd/app.py` file). If a local settings file is not found, Indexd falls back on the default settings.

There is specific information about some configuration options in the [distributed resolution](README.md#distributed-resolution-utilizing-prefixes-in-guids) section of this document.

## Testing

Be sure the prior steps for installation are already run.

Follow [installation](#installation) guidance and make sure your virtual environment is also activated.

You can then update the python dependencies and test from the repository root directory:

```console
python3 -m pytest -vv --cov=indexd --cov-report xml --junitxml="test-results.xml" tests
```

> If you're in `wsl1`, you may encounter an error such as `ImportMismatchError` when running pytest.  If this is the case, you can rename the `./tests/__pycache__` folder to `./tests/__pycache__Backup` and re-run the tests.
You may also need to update the [test settings](./tests/default_test_settings.py) with the appropriate database connection information prior to running the tests.

```python
settings["config"]["TEST_DB"] = "postgres://{username}:{password}@localhost:{port}/indexd_tests"
```

> If you are using Azure Postgresql, you will need to include the `username@hostname` for the `username` in the connection string.  You may also need to include support for SSL in the connection string, e.g. `postgres://{username@hostname}:{password}@serverfqdn:{port}/{dbname}?sslmode=require`.
> Further, you may run into `sqlite` errors; it may be helpful to rename existing local `*.sq3` files before running `pytest`.

## Administration

You can pass in the appropriate values for user setup using some of the helper scripts:

```console
python3 bin/index_admin.py create --username $username --password $password
python3 bin/index_admin.py delete --username $username
```

### Migrating the database

First, you'll want to make a local copy of the `bin/indexd_settings.py`:

```console
cp ./bin/indexd_settings.py ./bin/local_settings.py
```

Note, you'll also want to setup the settings in the `bin/creds.json` under the repository working directory:

```json
{
  "db_host": "localhost",
  "db_username": "indexdbuser",
  "db_password": "indexdpassword",
  "db_database": "indexd_db",
  "fence_database": "fence_db"
}
```

Of course, these settings will depend on where postgres is hosted and if the server is accessible to the context running the migration script.

With the appropriate settings, you can run the following command to migrate a database:

```console
python3 bin/index_admin.py migrate_database
```

If the `bin/local_settings.py` are not reachable, the script will fallback according to these [configuration notes](#configuration).