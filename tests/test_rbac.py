

def test_default_settings(app):
    """
    Test that the RBAC in default settings should be False.
    """
    from indexd import default_settings

    assert "RBAC" in default_settings.settings, "RBAC setting should be present in default settings"
    assert default_settings.settings["RBAC"] is False, "RBAC should be disabled by default"
