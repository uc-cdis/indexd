def test_default_settings():
    """
    Test that the RBAC in default settings should be False.
    """
    from indexd import default_settings

    assert (
        "config" in default_settings.settings
    ), "Config should be present in default settings"
    assert (
        "RBAC" in default_settings.settings["config"]
    ), "RBAC setting should be present in default settings"
    assert (
        default_settings.settings["config"]["RBAC"] is False
    ), "RBAC should be disabled by default"


def test_rbac_enabled(app_with_rbac):
    """
    Test that RBAC is enabled in the app.
    """
    assert (
        "RBAC" in app_with_rbac.config
    ), "RBAC setting should be present in app_with_rbac.config"

    assert app_with_rbac.config["RBAC"] is True, "RBAC should be enabled in the app"
