/* Entrypoint script to set up postgresql databases and users
based on a script from: https://github.com/uc-cdis/compose-services/blob/master/scripts/postgres_init.sql

See example usage in ADO pipeline.yaml (under ../../../azure-devops-pipeline.yaml)

This can also be used as part of a helm chart to setup the postgresql database (e.g. you can call this in from a shell script in a helm chart)
*/

CREATE DATABASE metadata_db;
CREATE DATABASE fence_db;
CREATE DATABASE indexd_db;
CREATE DATABASE arborist_db;

CREATE USER fence_user;
ALTER USER fence_user WITH PASSWORD 'fence_pass';
ALTER USER fence_user WITH SUPERUSER;

CREATE USER peregrine_user;
ALTER USER peregrine_user WITH PASSWORD 'peregrine_pass';
ALTER USER peregrine_user WITH SUPERUSER;

CREATE USER sheepdog_user;
ALTER USER sheepdog_user WITH PASSWORD 'sheepdog_pass';
ALTER USER sheepdog_user WITH SUPERUSER;

CREATE USER indexd_user;
ALTER USER indexd_user WITH PASSWORD 'indexd_pass';
ALTER USER indexd_user WITH SUPERUSER;

CREATE USER arborist_user;
ALTER USER arborist_user WITH PASSWORD 'arborist_pass';
ALTER USER arborist_user WITH SUPERUSER;

ALTER USER postgres WITH PASSWORD 'postgres';