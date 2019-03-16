# column name, data type, nullable, default value, primary key
INDEX_TABLES = {
    'base_version': [
        (u'baseid', u'character varying', u'NO', None, u'PRIMARY KEY'),
    ],
    'index_record': [
        (u'did', u'character varying', u'NO', None, u'PRIMARY KEY'),
        (u'baseid', u'character varying', u'YES', None, None),
        (u'rev', u'character varying', u'YES', None, None),
        (u'form', u'character varying', u'YES', None, None),
        (u'size', u'bigint',  u'YES', None, None),
        (u'created_date', u'timestamp without time zone', u'YES', None, None),
        (u'updated_date', u'timestamp without time zone', u'YES', None, None),
        (u'file_name', u'character varying', u'YES', None, None),
        (u'version', u'character varying', u'YES', None, None),
        (u'uploader', u'character varying', u'YES', None, None),
    ],
    'index_record_hash': [
        (u'did', u'character varying', 'NO', None, u'PRIMARY KEY'),
        (u'hash_type', u'character varying', 'NO', None, u'PRIMARY KEY'),
        (u'hash_value', u'character varying', 'YES', None, None),
    ],
    'index_record_url': [
        (u'did', u'character varying', 'NO', None, u'PRIMARY KEY'),
        (u'url', u'character varying', 'NO', None, u'PRIMARY KEY'),
    ],
    'index_schema_version': [
        (u'version', u'integer', 'NO', None, 'PRIMARY KEY'),
    ],
}

# column name, data type, nullable, default value, primary key
ALIAS_TABLES = {
    'alias_record': [
        (u'name', u'character varying', u'NO', None, u'PRIMARY KEY'),
        (u'rev', u'character varying', u'YES', None, None),
        (u'size', u'bigint',  u'YES', None, None),
        (u'release', u'character varying', u'YES', None, None),
        (u'metastring', u'character varying', u'YES', None, None),
        (u'keeper_authority', u'character varying', u'YES', None, None),
    ],
    'alias_record_hash': [
        (u'name', u'character varying', u'NO', None, u'PRIMARY KEY'),
        (u'hash_type', u'character varying', u'NO', None, u'PRIMARY KEY'),
        (u'hash_value', u'character varying', u'YES', None, None)
    ],
    'alias_record_host_authority': [
        (u'name', u'character varying', u'NO', None, u'PRIMARY KEY'),
        (u'host', u'character varying', u'NO', None, u'PRIMARY KEY'),
    ],
    'alias_schema_version': [
        (u'version', u'integer', u'NO', u"nextval('alias_schema_version_version_seq'::regclass)", u'PRIMARY KEY'),
    ],
}


def test_postgres_index_setup_tables(index_driver, database_conn):
    """
    Tests that the postgres index database gets set up correctly.
    """

    # postgres
    c = database_conn.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE'
    """)

    tables = [i[0] for i in c]

    for table in INDEX_TABLES:
        assert table in tables, '{table} not created'.format(table=table)

    for table, schema in INDEX_TABLES.items():
        # Index, column name, data type, nullable, default value, primary key
        c = database_conn.execute("""
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
            WHERE table_name = '{table}';
        """.format(table=table))

        assert schema == [i for i in c]


def test_postgres_alias_setup_tables(alias_driver, database_conn):
    """
    Tests that the postgres alias database gets set up correctly.
    """

    c = database_conn.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE'
    """)

    tables = [i[0] for i in c]

    for table in ALIAS_TABLES:
        assert table in tables, '{table} not created'.format(table=table)

    for table, schema in ALIAS_TABLES.items():
        # Index, column name, data type, nullable, default value, primary key
        c = database_conn.execute("""
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
            WHERE table_name = '{table}';
        """.format(table=table))

        assert schema == [i for i in c]
