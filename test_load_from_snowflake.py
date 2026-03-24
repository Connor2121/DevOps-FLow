import pytest
from unittest.mock import MagicMock, patch, call
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from load_from_snowflake import load_from_snowflake, _prepare_private_key


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def mock_spark():
    """Creates a mock SparkSession with chained read methods."""
    spark = MagicMock()
    reader = MagicMock()
    spark.read.format.return_value = reader
    reader.options.return_value = reader
    reader.option.return_value = reader
    reader.load.return_value = MagicMock(name="DataFrame")
    return spark


@pytest.fixture
def mock_dbutils():
    return MagicMock()


@pytest.fixture
def base_params(mock_spark, mock_dbutils):
    """Base parameters shared across most tests."""
    return {
        "spark": mock_spark,
        "dbutils": mock_dbutils,
        "url": "account.snowflakecomputing.com",
        "user": "test_user",
        "warehouse": "TEST_WH",
        "database": "TEST_DB",
        "schema": "TEST_SCHEMA",
    }


@pytest.fixture
def unencrypted_private_key():
    """Generates a real unencrypted PEM private key for testing."""
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    ).decode("utf-8")


@pytest.fixture
def encrypted_private_key():
    """Generates a real encrypted PEM private key for testing."""
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(b"testpassphrase")
    ).decode("utf-8")
    return pem


# ============================================================
# Validation Tests
# ============================================================

class TestInputValidation:

    def test_raises_when_both_table_and_query_provided(self, base_params):
        with pytest.raises(ValueError, match="Provide either table_name or query, not both"):
            load_from_snowflake(
                **base_params,
                table_name="MY_TABLE",
                query="SELECT * FROM MY_TABLE",
                password="pass123"
            )

    def test_raises_when_neither_table_nor_query_provided(self, base_params):
        with pytest.raises(ValueError, match="You must provide either table_name or query"):
            load_from_snowflake(
                **base_params,
                password="pass123"
            )

    def test_raises_when_no_auth_provided(self, base_params):
        with pytest.raises(ValueError, match="You must provide either password or private_key"):
            load_from_snowflake(
                **base_params,
                table_name="MY_TABLE"
            )

    def test_raises_when_both_auth_methods_provided(self, base_params, unencrypted_private_key):
        with pytest.raises(ValueError, match="Provide either password or private_key, not both"):
            load_from_snowflake(
                **base_params,
                table_name="MY_TABLE",
                password="pass123",
                private_key=unencrypted_private_key
            )


# ============================================================
# Password Authentication Tests
# ============================================================

class TestPasswordAuth:

    def test_full_table_read_with_password(self, base_params, mock_spark):
        df = load_from_snowflake(
            **base_params,
            table_name="MY_TABLE",
            password="pass123"
        )

        mock_spark.read.format.assert_called_once_with("net.snowflake.spark.snowflake")

        options_call = mock_spark.read.format.return_value.options.call_args[1]
        assert options_call["sfURL"] == "account.snowflakecomputing.com"
        assert options_call["sfUser"] == "test_user"
        assert options_call["sfPassword"] == "pass123"
        assert options_call["sfWarehouse"] == "TEST_WH"
        assert options_call["sfDatabase"] == "TEST_DB"
        assert options_call["sfSchema"] == "TEST_SCHEMA"
        assert "pem_private_key" not in options_call

        reader = mock_spark.read.format.return_value.options.return_value
        reader.option.assert_called_once_with("dbtable", "MY_TABLE")
        assert df is not None

    def test_query_with_password(self, base_params, mock_spark):
        query = "SELECT col1, col2 FROM MY_TABLE WHERE status = 'active'"
        df = load_from_snowflake(
            **base_params,
            query=query,
            password="pass123"
        )

        reader = mock_spark.read.format.return_value.options.return_value
        reader.option.assert_called_once_with("query", query)
        assert df is not None


# ============================================================
# Private Key Authentication Tests
# ============================================================

class TestPrivateKeyAuth:

    def test_full_table_read_with_unencrypted_key(self, base_params, mock_spark, unencrypted_private_key):
        df = load_from_snowflake(
            **base_params,
            table_name="MY_TABLE",
            private_key=unencrypted_private_key
        )

        options_call = mock_spark.read.format.return_value.options.call_args[1]
        assert options_call["pem_private_key"] == unencrypted_private_key
        assert "sfPassword" not in options_call
        assert df is not None

    def test_full_table_read_with_encrypted_key(self, base_params, mock_spark, encrypted_private_key):
        df = load_from_snowflake(
            **base_params,
            table_name="MY_TABLE",
            private_key=encrypted_private_key,
            private_key_passphrase="testpassphrase"
        )

        options_call = mock_spark.read.format.return_value.options.call_args[1]
        pem_key = options_call["pem_private_key"]
        assert "ENCRYPTED" not in pem_key
        assert "BEGIN PRIVATE KEY" in pem_key
        assert df is not None

    def test_query_with_private_key(self, base_params, mock_spark, unencrypted_private_key):
        query = "SELECT * FROM MY_TABLE"
        df = load_from_snowflake(
            **base_params,
            query=query,
            private_key=unencrypted_private_key
        )

        reader = mock_spark.read.format.return_value.options.return_value
        reader.option.assert_called_once_with("query", query)
        assert df is not None


# ============================================================
# Optional Role Tests
# ============================================================

class TestRoleOption:

    def test_role_included_when_provided(self, base_params, mock_spark):
        load_from_snowflake(
            **base_params,
            table_name="MY_TABLE",
            password="pass123",
            role="DATA_READER"
        )

        options_call = mock_spark.read.format.return_value.options.call_args[1]
        assert options_call["sfRole"] == "DATA_READER"

    def test_role_excluded_when_not_provided(self, base_params, mock_spark):
        load_from_snowflake(
            **base_params,
            table_name="MY_TABLE",
            password="pass123"
        )

        options_call = mock_spark.read.format.return_value.options.call_args[1]
        assert "sfRole" not in options_call


# ============================================================
# _prepare_private_key Tests
# ============================================================

class TestPreparePrivateKey:

    def test_unencrypted_key_passes_through(self, unencrypted_private_key):
        result = _prepare_private_key(unencrypted_private_key)
        assert result == unencrypted_private_key

    def test_encrypted_key_is_decrypted(self, encrypted_private_key):
        result = _prepare_private_key(encrypted_private_key, "testpassphrase")
        assert "ENCRYPTED" not in result
        assert "BEGIN PRIVATE KEY" in result

    def test_encrypted_key_without_passphrase_raises(self, encrypted_private_key):
        with pytest.raises(ValueError, match="Private key is encrypted but no passphrase was provided"):
            _prepare_private_key(encrypted_private_key)

    def test_encrypted_key_with_wrong_passphrase_raises(self, encrypted_private_key):
        with pytest.raises(Exception):
            _prepare_private_key(encrypted_private_key, "wrongpassphrase")

    def test_decrypted_key_is_valid_pem(self, encrypted_private_key):
        result = _prepare_private_key(encrypted_private_key, "testpassphrase")
        # Verify the result can be loaded back as a valid private key
        key = serialization.load_pem_private_key(
            result.encode(),
            password=None,
            backend=default_backend()
        )
        assert key is not None
