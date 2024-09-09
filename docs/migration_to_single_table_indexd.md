# Running Data Migration for Single Table Indexd

## A. Prepare Database and Configuration
1. **Deploy the version of IndexD** that contains Single Table Indexd. Alembic, used for database migrations, should create a new table named `records` in the IndexD database. Note that this is a database migration and NOT a data migration.
2. **Create clone database:**
```
    # create a new database
    gen3 psql indexd -c 'create database indexd_new'
    # dump old db and restore it on the new one
    gen3 db backup indexd | psql -U $indexd_user -h <hostname> -d indexd_new
```

**If you don’t have permissions:**

a. Run `gen3 db creds indexd`

b. Using the information from above, run `gen3 psql $g3Farmserver -c "alter user $username createdb;"`

3. **Update credentials:** After Creating the backup database, update the `Gen3Secrets/creds.json` to include the credentials for the new database. Add a new block named `indexd_new` with the credentials for the new database. Copy configuration from indexd. Run `gen3 kube-setup-secrets` to create the secrets in kube secrets. The new database can be accessed by using `gen3 psql indexd_new`

4. **Update cloud automation script:** `~/cloud-automation/kube/services/indexd/indexd-deploy.yaml`
```
        volumes:
        - name: creds-volume-new
        secret:
            secretName: "indexd_new"
        volumeMounts:
        - name: "creds-volume-new"
        readOnly: True
        mountPath: "var/www/indexd/new_creds.json"
       subPath: creds.json
```
After updating the cloud-auto script, run `gen3 roll indexd`

## B. Run Database Migration
Run the cloud-automation migration job, `indexd-single-table-migration-job.yaml`

To run:
```
    gen3 job run indexd-single-table-migration-job
```

**If a job stops in the middle of migration** for any reason, the job should return the last seen guid; re-run the job with the `START_DID` parameter:

```
    gen3 job run indexd-single-table-migration-job START_DID <guid>
```

## C. Swap IndexD to use the clone database:
Go to the indexd settings under `cloud-automation/apis_config/indexd_settings.py`
Change the config `CONFIG[“INDEX”]`

**From:**
```
    CONFIG["INDEX"] = {
        "driver": SQLAlchemyIndexDriver(
        "postgresql://postgres:postgres@localhost:5432/indexd_tests",
            echo=True,
            index_config={
                "DEFAULT_PREFIX": "testprefix:",
                "PREPEND_PREFIX": True,
                "ADD_PREFIX_ALIAS": False,
            },
        )
    }
```

**To:**
```
    from indexd.index.drivers.single_table_alchemy import SingleTableSQLAlchemyIndexDriver

    USE_SINGLE_TABLE = True

    if USE_SINGLE_TABLE is True:
        CONFIG["INDEX"] = {
            "driver": SingleTableSQLAlchemyIndexDriver(
                "postgresql://postgres:postgres@localhost:5432/indexd_tests",
                echo=True,
                index_config={
                    "DEFAULT_PREFIX": "testprefix:",
                    "PREPEND_PREFIX": True,
                    "ADD_PREFIX_ALIAS": False,
                },
            )
        }
    else:
        CONFIG["INDEX"] = {
            "driver": SQLAlchemyIndexDriver(
            "postgresql://postgres:postgres@localhost:5432/indexd_tests",
                echo=True,
                index_config={
                    "DEFAULT_PREFIX": "testprefix:",
                    "PREPEND_PREFIX": True,
                    "ADD_PREFIX_ALIAS": False,
                },
            )
        }
```

Import `from indexd.index.drivers.single_table_alchemy import SingleTableSQLAlchemyIndexDriver` and add the driver `SingleTableSQLAlchemyIndexDriver` similar to `SQLAlchemyIndexDriver`. Wrap those around an `if` statement like shown above and add a new configuration `USE_SINGLE_TABLE` to make it easier to swap between the two drivers.

## D. Swap the current running database with the snapshot:
In `creds.json`, you should have an indexd block and an indexd_new block. Swap them out, `gen3 kube-setup-secrets` and `gen3 roll indexd`
