import pytest
from indexd.auth.discovery_context import authorize_discovery, set_auth_context, reset_auth_context


class MyExampleDriver:
    """
    Example driver class with methods using the authorize_discovery decorator.
    """

    @authorize_discovery
    def ids(
        self,
        limit: int = 100,
        can_user_discover: bool = None,
        authorized_resources: list = None,
    ) -> dict:
        """
        Returns a dictionary of parameters, including injected context values.
        """
        return dict(
            limit=limit,
            can_user_discover=can_user_discover,
            authorized_resources=authorized_resources,
        )

    @authorize_discovery
    async def ids_async(
        self,
        limit: int = 100,
        can_user_discover: bool = None,
        authorized_resources: list = None,
    ) -> dict:
        """
        Async version returning a dictionary of parameters, including injected context values.
        """
        return dict(
            limit=limit,
            can_user_discover=can_user_discover,
            authorized_resources=authorized_resources,
        )

    @authorize_discovery
    def missing_parm_ids(
        self,
        limit: int = 100,
        # can_user_discover: bool = None,
        authorized_resources: list = None,
    ) -> dict:
        """
        Returns a dictionary of parameters, including injected context values.
        """
        return dict(
            limit=limit,
            authorized_resources=authorized_resources,
        )


@pytest.fixture
def repo() -> MyExampleDriver:
    """
    Pytest fixture returning an instance of MyExampleDriver.
    """
    return MyExampleDriver()


@pytest.fixture
def set_ctx():
    """
    Pytest fixture to set request_ctx safely and reset after the test.
    Usage:
        set_ctx(can_user_discover=True, authorized_resources=["/a", "/b"])
    """

    def _setter(**payload):
        """
        Sets the request context with the given payload.
        """
        set_auth_context(**payload)

    yield _setter
    reset_auth_context()


@pytest.fixture
async def set_ctx_async():
    """
    Async pytest fixture to set request_ctx safely and reset after the test.
    Usage:
        set_ctx_async(can_user_discover=True, authorized_resources=["/a", "/b"])
    """

    def _setter(**payload):
        """
        Sets the request context with the given payload.
        """
        set_auth_context(**payload)

    yield _setter
    reset_auth_context()


def test_injects_when_missing(repo: MyExampleDriver, set_ctx):
    """
    Test that context values are injected when parameters are missing.
    """
    set_ctx(
        can_user_discover=True, authorized_resources=["/programs/XYZ", "/studies/ABC"]
    )
    result = repo.ids()
    assert result["can_user_discover"] is True
    assert result["authorized_resources"] == ["/programs/XYZ", "/studies/ABC"]


def test_does_not_override_explicit_values(repo: MyExampleDriver, set_ctx):
    """
    Test that explicit parameter values are not overridden by context.
    """
    set_ctx(can_user_discover=True, authorized_resources=["/ctx"])
    result = repo.ids(
        can_user_discover=False,
        authorized_resources=["/explicit"],
    )
    assert result["can_user_discover"] is False
    assert result["authorized_resources"] == ["/explicit"]


def test_injects_when_none(repo: MyExampleDriver, set_ctx):
    """
    Test that context values are injected when parameters are None.
    """
    set_ctx(can_user_discover=True, authorized_resources=["/from_ctx"])
    result = repo.ids(can_user_discover=None, authorized_resources=None)
    assert result["can_user_discover"] is True
    assert result["authorized_resources"] == ["/from_ctx"]


def test_no_injection_if_context_empty(repo: MyExampleDriver):
    """
    Test that no injection occurs if context is empty.
    """
    result = repo.ids()
    assert result["can_user_discover"] is None
    assert result["authorized_resources"] is None


@pytest.mark.asyncio
async def test_async_function_injection(repo: MyExampleDriver, set_ctx_async):
    """
    Test async context injection for the ids_async method.
    """
    c = 0
    async for setter in set_ctx_async:  # noqa: Expected type 'collections.AsyncIterable', got '(payload: dict[str, Any]) -> None' instead
        setter(can_user_discover=True, authorized_resources=["/async"])
        result = await repo.ids_async()
        assert result["can_user_discover"] is True
        assert result["authorized_resources"] == ["/async"]
        c += 1
    assert c == 1


def test_positional_args_still_work(repo: MyExampleDriver, set_ctx):
    """
    Test that positional arguments are preserved and context injection works.
    """
    set_ctx(can_user_discover=True, authorized_resources=["/positional"])
    result = repo.ids(50, can_user_discover=False, authorized_resources=["/mine"])
    assert result["limit"] == 50
    assert result["authorized_resources"] == ["/mine"]
    assert result["can_user_discover"] is False


def test_missing_parm(repo: MyExampleDriver, set_ctx):
    """
    Test that a function with a missing parameter still gets context injection for existing parameters.
    """
    set_ctx(can_user_discover=True, authorized_resources=["/positional"])
    result = repo.missing_parm_ids(50, authorized_resources=["/mine"])
    assert result["limit"] == 50
    assert result["authorized_resources"] == ["/mine"]
    with pytest.raises(KeyError):
        _ = result["can_user_discover"]
