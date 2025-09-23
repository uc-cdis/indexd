def test_default_settings():
    """
    Test that the RBAC in default settings should be False.
    """
    from indexd import default_settings

    assert (
        "config" in default_settings.settings
    ), "Config should be present in default settings"
    assert (
        "ARE_RECORDS_DISCOVERABLE" in default_settings.settings["config"]
    ), "ARE_RECORDS_DISCOVERABLE setting should be present in default settings"
    assert (
        default_settings.settings["config"]["ARE_RECORDS_DISCOVERABLE"] is True
    ), "ARE_RECORDS_DISCOVERABLE should be enabled by default"

    assert (
        "GLOBAL_DISCOVERY_AUTHZ" in default_settings.settings["config"]
    ), "GLOBAL_DISCOVERY_AUTHZ setting should be present in default settings"
    assert (
        default_settings.settings["config"]["GLOBAL_DISCOVERY_AUTHZ"] == []
    ), "GLOBAL_DISCOVERY_AUTHZ should be an empty list by default"


def test_rbac_enabled(app_with_rbac):
    """
    Test that RBAC is enabled in the app.
    """
    assert (
        "ARE_RECORDS_DISCOVERABLE" in app_with_rbac.config
    ), "ARE_RECORDS_DISCOVERABLE setting should be present in app_with_rbac.config"

    assert (
        app_with_rbac.config["ARE_RECORDS_DISCOVERABLE"] is False
    ), "RBAC should be enabled in the app"


def test_global_discovery_authz_enabled(app_with_rbac):
    """
    Test that RBAC is enabled in the app.
    """
    assert (
        "GLOBAL_DISCOVERY_AUTHZ" in app_with_rbac.config
    ), "GLOBAL_DISCOVERY_AUTHZ setting should be present in app_with_rbac.config"

    assert (
        len(app_with_rbac.config["GLOBAL_DISCOVERY_AUTHZ"]) > 0
    ), "GLOBAL_DISCOVERY_AUTHZ should be set in the app"
