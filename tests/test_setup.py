# column name, data type, nullable, default value, primary key
INDEX_TABLES = {
    "index_record": [
        ("did", "character varying", "NO", None, "PRIMARY KEY"),
        ("baseid", "character varying", "YES", None, None),
        ("rev", "character varying", "YES", None, None),
        ("form", "character varying", "YES", None, None),
        ("size", "bigint", "YES", None, None),
        ("release_number", "character varying", "YES", None, None),
        ("created_date", "timestamp without time zone", "YES", None, None),
        ("updated_date", "timestamp without time zone", "YES", None, None),
        ("file_name", "character varying", "YES", None, None),
        ("version", "character varying", "YES", None, None),
        ("uploader", "character varying", "YES", None, None),
        ("index_metadata", "jsonb", "YES", None, None),
    ],
    "index_record_ace": [
        ("did", "character varying", "NO", None, "PRIMARY KEY"),
        ("ace", "character varying", "NO", None, "PRIMARY KEY"),
    ],
    "index_record_hash": [
        ("did", "character varying", "NO", None, "PRIMARY KEY"),
        ("hash_type", "character varying", "NO", None, "PRIMARY KEY"),
        ("hash_value", "character varying", "YES", None, None),
    ],
    "index_record_url_metadata_jsonb": [
        ("did", "character varying", "NO", None, "PRIMARY KEY"),
        ("url", "character varying", "NO", None, "PRIMARY KEY"),
        ("type", "character varying", "YES", None, None),
        ("state", "character varying", "YES", None, None),
        ("urls_metadata", "jsonb", "YES", None, None),
    ],
    "index_schema_version": [("version", "integer", "NO", None, "PRIMARY KEY"),],
}

# column name, data type, nullable, default value, primary key
ALIAS_TABLES = {
    "alias_record": [
        ("name", "character varying", "NO", None, "PRIMARY KEY"),
        ("rev", "character varying", "YES", None, None),
        ("size", "bigint", "YES", None, None),
        ("release", "character varying", "YES", None, None),
        ("metastring", "character varying", "YES", None, None),
        ("keeper_authority", "character varying", "YES", None, None),
    ],
    "alias_record_hash": [
        ("name", "character varying", "NO", None, "PRIMARY KEY"),
        ("hash_type", "character varying", "NO", None, "PRIMARY KEY"),
        ("hash_value", "character varying", "YES", None, None),
    ],
    "alias_record_host_authority": [
        ("name", "character varying", "NO", None, "PRIMARY KEY"),
        ("host", "character varying", "NO", None, "PRIMARY KEY"),
    ],
    "alias_schema_version": [
        (
            "version",
            "integer",
            "NO",
            "nextval('alias_schema_version_version_seq'::regclass)",
            "PRIMARY KEY",
        ),
    ],
}


def test_postgres_index_setup_tables(index_driver, database_conn):
    """
    Tests that the postgres index database gets set up correctly.
    """

    # postgres
    c = database_conn.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE'
    """
    )

    tables = [i[0] for i in c]

    for table in INDEX_TABLES:
        assert table in tables, f"{table} not created"

    for table, schema in INDEX_TABLES.items():
        # Index, column name, data type, nullable, default value, primary key
        c = database_conn.execute(
            """
            SELECT col.column_name, col.data_type, col.is_nullable,
                col.column_default, c.constraint_type
            FROM information_schema.columns col
            left JOIN (
                SELECT column_name, constraint_type
                FROM information_schema.table_constraints
                NATURAL JOIN information_schema.constraint_table_usage
                NATURAL JOIN information_schema.constraint_column_usage
                WHERE table_name = '{table}'
                ) c
            ON col.column_name =  c.column_name
            WHERE table_name = '{table}'
        """.format(
                table=table
            )
        )

        assert schema == [i for i in c]


def test_postgres_alias_setup_tables(alias_driver, database_conn):
    """
    Tests that the postgres alias database gets set up correctly.
    """

    c = database_conn.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE'
    """
    )

    tables = [i[0] for i in c]

    for table in ALIAS_TABLES:
        assert table in tables, f"{table} not created"

    for table, schema in ALIAS_TABLES.items():
        # Index, column name, data type, nullable, default value, primary key
        c = database_conn.execute(
            """
            SELECT col.column_name, col.data_type, col.is_nullable,
                col.column_default, c.constraint_type
            FROM information_schema.columns col
            left JOIN (
                SELECT column_name, constraint_type
                FROM information_schema.table_constraints
                NATURAL JOIN information_schema.constraint_table_usage
                NATURAL JOIN information_schema.constraint_column_usage
                WHERE table_name = '{table}'
                ) c
            ON col.column_name =  c.column_name
            WHERE table_name = '{table}'
        """.format(
                table=table
            )
        )

        assert schema == [i for i in c]
