from datus_bi_core.models import AuthType
from datus_bi_core.registry import BIAdaptorRegistry


class MockAdaptor:
    pass


def test_register_and_get():
    # Save original state
    orig_adaptors = BIAdaptorRegistry._adaptors.copy()
    orig_metadata = BIAdaptorRegistry._metadata.copy()
    orig_init = BIAdaptorRegistry._initialized
    try:
        BIAdaptorRegistry._initialized = True  # Skip discovery
        BIAdaptorRegistry.register(
            "mock_platform",
            MockAdaptor,
            auth_type=AuthType.LOGIN,
            display_name="Mock",
            capabilities={"list_dashboards"},
        )
        assert BIAdaptorRegistry.get("mock_platform") is MockAdaptor
    finally:
        BIAdaptorRegistry._adaptors = orig_adaptors
        BIAdaptorRegistry._metadata = orig_metadata
        BIAdaptorRegistry._initialized = orig_init


def test_get_capabilities():
    orig_adaptors = BIAdaptorRegistry._adaptors.copy()
    orig_metadata = BIAdaptorRegistry._metadata.copy()
    orig_init = BIAdaptorRegistry._initialized
    try:
        BIAdaptorRegistry._initialized = True
        BIAdaptorRegistry.register(
            "mock2",
            MockAdaptor,
            auth_type=AuthType.API_KEY,
            capabilities={"dashboard_write", "chart_write"},
        )
        caps = BIAdaptorRegistry.get_capabilities("mock2")
        assert "dashboard_write" in caps
        assert "chart_write" in caps
        assert len(caps) == 2
    finally:
        BIAdaptorRegistry._adaptors = orig_adaptors
        BIAdaptorRegistry._metadata = orig_metadata
        BIAdaptorRegistry._initialized = orig_init


def test_list_adaptors():
    orig_adaptors = BIAdaptorRegistry._adaptors.copy()
    orig_metadata = BIAdaptorRegistry._metadata.copy()
    orig_init = BIAdaptorRegistry._initialized
    try:
        BIAdaptorRegistry._initialized = True
        BIAdaptorRegistry._adaptors = {}
        BIAdaptorRegistry._metadata = {}
        BIAdaptorRegistry.register(
            "mock_list", MockAdaptor, auth_type=AuthType.LOGIN
        )
        all_adaptors = BIAdaptorRegistry.list_adaptors()
        assert "mock_list" in all_adaptors
        assert all_adaptors["mock_list"] is MockAdaptor
    finally:
        BIAdaptorRegistry._adaptors = orig_adaptors
        BIAdaptorRegistry._metadata = orig_metadata
        BIAdaptorRegistry._initialized = orig_init


def test_is_registered():
    orig_adaptors = BIAdaptorRegistry._adaptors.copy()
    orig_metadata = BIAdaptorRegistry._metadata.copy()
    orig_init = BIAdaptorRegistry._initialized
    try:
        BIAdaptorRegistry._initialized = True
        BIAdaptorRegistry._adaptors = {}
        BIAdaptorRegistry._metadata = {}
        BIAdaptorRegistry.register(
            "mock_check", MockAdaptor, auth_type=AuthType.LOGIN
        )
        assert BIAdaptorRegistry.is_registered("mock_check") is True
        assert BIAdaptorRegistry.is_registered("nonexistent") is False
    finally:
        BIAdaptorRegistry._adaptors = orig_adaptors
        BIAdaptorRegistry._metadata = orig_metadata
        BIAdaptorRegistry._initialized = orig_init


def test_get_metadata():
    orig_adaptors = BIAdaptorRegistry._adaptors.copy()
    orig_metadata = BIAdaptorRegistry._metadata.copy()
    orig_init = BIAdaptorRegistry._initialized
    try:
        BIAdaptorRegistry._initialized = True
        BIAdaptorRegistry._adaptors = {}
        BIAdaptorRegistry._metadata = {}
        BIAdaptorRegistry.register(
            "mock_meta",
            MockAdaptor,
            auth_type=AuthType.API_KEY,
            display_name="Mock Meta",
            capabilities={"read"},
        )
        meta = BIAdaptorRegistry.get_metadata("mock_meta")
        assert meta is not None
        assert meta.platform == "mock_meta"
        assert meta.auth_type == AuthType.API_KEY
        assert meta.display_name == "Mock Meta"
        assert meta.capabilities == {"read"}
        assert BIAdaptorRegistry.get_metadata("nonexistent") is None
    finally:
        BIAdaptorRegistry._adaptors = orig_adaptors
        BIAdaptorRegistry._metadata = orig_metadata
        BIAdaptorRegistry._initialized = orig_init


def test_register_empty_platform():
    orig_adaptors = BIAdaptorRegistry._adaptors.copy()
    orig_metadata = BIAdaptorRegistry._metadata.copy()
    orig_init = BIAdaptorRegistry._initialized
    try:
        BIAdaptorRegistry._initialized = True
        BIAdaptorRegistry._adaptors = {}
        BIAdaptorRegistry._metadata = {}
        BIAdaptorRegistry.register("", MockAdaptor, auth_type=AuthType.LOGIN)
        assert BIAdaptorRegistry.get("") is None
    finally:
        BIAdaptorRegistry._adaptors = orig_adaptors
        BIAdaptorRegistry._metadata = orig_metadata
        BIAdaptorRegistry._initialized = orig_init
