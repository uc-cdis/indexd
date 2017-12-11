from indexd.index.drivers.alchemy import (
    SQLAlchemyIndexDriver, IndexSchemaVersion)

from indexd.alias.drivers.alchemy import (
    SQLAlchemyAliasDriver, AliasSchemaVersion)


def test_migrate_index(monkeypatch):
    called = []

    def mock_migrate(**kwargs):
        called.append(True)

    monkeypatch.setattr(
        'indexd.index.drivers.alchemy.CURRENT_SCHEMA_VERSION', 2)
    monkeypatch.setattr(
         'indexd.utils.check_engine_for_migrate',
         lambda _: True
    )
    monkeypatch.setattr(
        'indexd.index.drivers.alchemy.SCHEMA_MIGRATION_FUNCTIONS',
        [mock_migrate, mock_migrate])

    driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')
    assert len(called) == 2
    with driver.session as s:
        v = s.query(IndexSchemaVersion).first()
        assert v.version == 2
        s.delete(v)

def test_migrate_index_only_diff(monkeypatch):
    called = []

    def mock_migrate(**kwargs):
        called.append(True)

    called_2 = []
    def mock_migrate_2(**kwargs):
        called_2.append(True)

    monkeypatch.setattr(
         'indexd.utils.check_engine_for_migrate',
         lambda _: True
    )
    monkeypatch.setattr(
        'indexd.index.drivers.alchemy.CURRENT_SCHEMA_VERSION', 1)
    monkeypatch.setattr(
        'indexd.index.drivers.alchemy.SCHEMA_MIGRATION_FUNCTIONS',
        [mock_migrate, mock_migrate_2])

    driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')
    assert len(called) == 1
    assert len(called_2) == 0

    called = []
    called_2 = []
    monkeypatch.setattr(
        'indexd.index.drivers.alchemy.CURRENT_SCHEMA_VERSION', 2)

    driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')
    assert len(called) == 0
    assert len(called_2) == 1

    with driver.session as s:
        v = s.query(IndexSchemaVersion).first()
        assert v.version == 2
        s.delete(v)

def test_migrate_alias(monkeypatch):
    called = []

    def mock_migrate(**kwargs):
        called.append(True)

    monkeypatch.setattr(
        'indexd.alias.drivers.alchemy.CURRENT_SCHEMA_VERSION', 1)
    monkeypatch.setattr(
        'indexd.alias.drivers.alchemy.SCHEMA_MIGRATION_FUNCTIONS',
        [mock_migrate])

    monkeypatch.setattr(
         'indexd.utils.check_engine_for_migrate',
         lambda _: True
    )

    driver = SQLAlchemyAliasDriver('sqlite:///alias.sq3')
    assert len(called) == 1
    with driver.session as s:
        v = s.query(AliasSchemaVersion).first()
        assert v.version == 1
        s.delete(v)
