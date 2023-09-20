from indexd import default_settings
from indexd.index.drivers.single_table_alchemy import SingleTableSQLAlchemyIndexDriver

# - DEFAULT_PREFIX: prefix to be prepended.
# - PREPEND_PREFIX: the prefix is preprended to the generated GUID when a
#   new record is created WITHOUT a provided GUID.
# - ADD_PREFIX_ALIAS: aliases are created for new records - "<PREFIX><GUID>".
# Do NOT set both ADD_PREFIX_ALIAS and PREPEND_PREFIX to True, or aliases
# will be created as "<PREFIX><PREFIX><GUID>".
default_settings.settings["config"]["INDEX"] = {
    "driver": SingleTableSQLAlchemyIndexDriver(
        "sqlite:///index.sq3",
        echo=True,
        index_config={
            "DEFAULT_PREFIX": "testprefix:",
            "PREPEND_PREFIX": True,
            "ADD_PREFIX_ALIAS": False,
        },
    )
}
settings = default_settings.settings
