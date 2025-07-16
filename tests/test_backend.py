from __future__ import annotations

import json
import shutil
from decimal import Decimal
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from meltano.core.project import Project
from meltano.core.state_store import MeltanoState, state_store_manager_from_project_settings
from meltano.core.state_store.base import (
    MissingStateBackendSettingsError,
    StateIDLockedError,
)

from meltano_state_backend_snowflake.backend import SnowflakeStateStoreManager

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path


@pytest.fixture
def project(tmp_path: Path) -> Project:
    path = tmp_path / "project"
    shutil.copytree(
        "fixtures/project",
        path,
        ignore=shutil.ignore_patterns(".meltano/**"),
    )
    return Project.find(path.resolve())  # type: ignore[no-any-return]


def test_get_manager(project: Project) -> None:
    with mock.patch(
        "meltano_state_backend_snowflake.backend.SnowflakeStateStoreManager._ensure_tables",
    ) as mock_ensure_tables:
        manager = state_store_manager_from_project_settings(project.settings)

    mock_ensure_tables.assert_called_once()
    assert isinstance(manager, SnowflakeStateStoreManager)
    assert manager.uri == "snowflake://my-account"
    assert manager.account == "my-account"
    assert manager.user == "test_user"
    assert manager.password == "test_password"  # noqa: S105
    assert manager.warehouse == "test_warehouse"
    assert manager.database == "test_database"
    assert manager.schema == "test_schema"
    assert manager.role == "test_role"


@pytest.mark.parametrize(
    ("setting_name", "env_var_name"),
    (
        pytest.param(
            "state_backend.snowflake.warehouse",
            "MELTANO_STATE_BACKEND_SNOWFLAKE_WAREHOUSE",
            id="warehouse",
        ),
        pytest.param(
            "state_backend.snowflake.database",
            "MELTANO_STATE_BACKEND_SNOWFLAKE_DATABASE",
            id="database",
        ),
        pytest.param(
            "state_backend.snowflake.schema",
            "MELTANO_STATE_BACKEND_SNOWFLAKE_SCHEMA",
            id="schema",
        ),
        pytest.param(
            "state_backend.snowflake.role",
            "MELTANO_STATE_BACKEND_SNOWFLAKE_ROLE",
            id="role",
        ),
    ),
)
def test_settings(project: Project, setting_name: str, env_var_name: str) -> None:
    setting = project.settings.find_setting(setting_name)
    env_vars = setting.env_vars(prefixes=["meltano"])
    assert env_vars[0].key == env_var_name


@pytest.fixture
def mock_connection() -> Generator[tuple[mock.Mock, mock.Mock], None, None]:
    """Mock Snowflake connection."""
    with mock.patch("snowflake.connector.connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()

        # Mock the context manager for cursor
        mock_cursor_context = mock.Mock()
        mock_cursor_context.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_cursor_context.__exit__ = mock.Mock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor_context

        mock_connect.return_value = mock_conn
        yield mock_conn, mock_cursor


@pytest.fixture
def subject(
    mock_connection: tuple[mock.Mock, mock.Mock],
) -> tuple[SnowflakeStateStoreManager, mock.Mock]:
    """Create SnowflakeStateStoreManager instance with mocked connection."""
    mock_conn, mock_cursor = mock_connection
    manager = SnowflakeStateStoreManager(
        uri="snowflake://testuser:testpass@testaccount/testdb/testschema",
        account="testaccount",
        user="testuser",
        password="testpass",  # noqa: S106
        warehouse="testwarehouse",
        database="testdb",
        schema="testschema",
    )
    # Replace the cached connection with our mock
    manager.connection = mock_conn
    return manager, mock_cursor


def test_set_state(
    subject: tuple[SnowflakeStateStoreManager, mock.Mock],
) -> None:
    """Test setting state."""
    manager, mock_cursor = subject

    # Test setting new state
    state = MeltanoState(
        state_id="test_job",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    manager.set(state)

    # Verify MERGE query was executed with fully qualified table name
    mock_cursor.execute.assert_called()
    call_args = mock_cursor.execute.call_args
    assert "MERGE INTO testdb.testschema.meltano_state" in call_args[0][0]
    assert call_args[0][1] == (
        "test_job",
        json.dumps({"singer_state": {"partial": 1}}),
        json.dumps({"singer_state": {"complete": 1}}),
    )


def test_get_state(
    subject: tuple[SnowflakeStateStoreManager, mock.Mock],
) -> None:
    """Test getting state."""
    manager, mock_cursor = subject

    # Mock cursor response - Snowflake VARIANT columns return Python dicts
    mock_cursor.fetchone.return_value = (
        {"singer_state": {"partial": 1}},
        {"singer_state": {"complete": 1}},
    )

    # Get state
    state = manager.get("test_job")
    assert state is not None

    # Verify query with fully qualified table name
    mock_cursor.execute.assert_called()
    call_args = mock_cursor.execute.call_args
    assert "FROM testdb.testschema.meltano_state" in call_args[0][0]
    assert call_args[0][1] == ("test_job",)

    # Verify returned state
    assert state.state_id == "test_job"
    assert state.partial_state == {"singer_state": {"partial": 1}}
    assert state.completed_state == {"singer_state": {"complete": 1}}


def test_get_state_with_json_strings(
    subject: tuple[SnowflakeStateStoreManager, mock.Mock],
) -> None:
    """Test getting state when Snowflake returns JSON strings."""
    manager, mock_cursor = subject

    # Mock cursor response with JSON strings (as Snowflake might return)
    mock_cursor.fetchone.return_value = (
        '{"singer_state": {"partial": 1}}',
        '{"singer_state": {"complete": 1}}',
    )

    # Get state
    state = manager.get("test_job")
    assert state is not None

    # Verify returned state is properly parsed
    assert state.state_id == "test_job"
    assert state.partial_state == {"singer_state": {"partial": 1}}
    assert state.completed_state == {"singer_state": {"complete": 1}}


def test_get_state_with_null_values(
    subject: tuple[SnowflakeStateStoreManager, mock.Mock],
) -> None:
    """Test getting state with NULL VARIANT columns."""
    manager, mock_cursor = subject

    # Mock cursor response with None values
    mock_cursor.fetchone.return_value = (
        None,
        '{"singer_state": {"complete": 1}}',
    )

    # Get state
    state = manager.get("test_job")
    assert state is not None

    # Verify returned state handles None correctly
    assert state.state_id == "test_job"
    assert state.partial_state == {}
    assert state.completed_state == {"singer_state": {"complete": 1}}


def test_get_state_not_found(
    subject: tuple[SnowflakeStateStoreManager, mock.Mock],
) -> None:
    """Test getting state that doesn't exist."""
    manager, mock_cursor = subject

    # Mock cursor response
    mock_cursor.fetchone.return_value = None

    # Get state
    state = manager.get("nonexistent")

    # Verify it returns None
    assert state is None


def test_get_state_with_none_values(
    subject: tuple[SnowflakeStateStoreManager, mock.Mock],
) -> None:
    """Test getting state with None values (NULL in Snowflake)."""
    manager, mock_cursor = subject

    # Mock cursor response with None values
    mock_cursor.fetchone.return_value = (None, None)

    # Get state
    state = manager.get("test_job")
    assert state is not None

    # Verify returned state has empty dicts for None values
    assert state.state_id == "test_job"
    assert state.partial_state == {}
    assert state.completed_state == {}


def test_delete_state(
    subject: tuple[SnowflakeStateStoreManager, mock.Mock],
) -> None:
    """Test deleting state."""
    manager, mock_cursor = subject

    # Delete state
    manager.delete("test_job")

    # Verify DELETE query with fully qualified table name
    mock_cursor.execute.assert_called()
    call_args = mock_cursor.execute.call_args
    assert "DELETE FROM testdb.testschema.meltano_state" in call_args[0][0]
    assert call_args[0][1] == ("test_job",)


def test_get_state_ids(
    subject: tuple[SnowflakeStateStoreManager, mock.Mock],
) -> None:
    """Test getting all state IDs."""
    manager, mock_cursor = subject

    # Mock cursor response - need to make cursor itself iterable
    mock_cursor.__iter__ = mock.Mock(
        return_value=iter([("job1",), ("job2",), ("job3",)]),
    )

    # Get state IDs
    state_ids = list(manager.get_state_ids())

    # Verify query with fully qualified table name (skip table creation calls)
    select_calls = [
        call for call in mock_cursor.execute.call_args_list if "SELECT state_id FROM" in call[0][0]
    ]
    assert len(select_calls) == 1
    assert "SELECT state_id FROM testdb.testschema.meltano_state" in select_calls[0][0][0]

    # Verify returned IDs
    assert state_ids == ["job1", "job2", "job3"]


def test_get_state_ids_with_pattern(
    subject: tuple[SnowflakeStateStoreManager, mock.Mock],
) -> None:
    """Test getting state IDs with pattern."""
    manager, mock_cursor = subject

    # Mock cursor response - need to make cursor itself iterable
    mock_cursor.__iter__ = mock.Mock(
        return_value=iter([("test_job_1",), ("test_job_2",)]),
    )

    # Get state IDs with pattern
    state_ids = list(manager.get_state_ids("test_*"))

    # Verify query with LIKE and fully qualified table name (skip table creation calls)  # noqa: E501
    select_calls = [
        call for call in mock_cursor.execute.call_args_list if "SELECT state_id FROM" in call[0][0]
    ]
    assert len(select_calls) == 1
    assert (
        "SELECT state_id FROM testdb.testschema.meltano_state WHERE state_id LIKE"
        in select_calls[0][0][0]
    )
    assert select_calls[0][0][1] == ("test_%",)

    # Verify returned IDs
    assert state_ids == ["test_job_1", "test_job_2"]


def test_clear_all(
    subject: tuple[SnowflakeStateStoreManager, mock.Mock],
) -> None:
    """Test clearing all states."""
    manager, mock_cursor = subject

    # Mock count query response
    mock_cursor.fetchone.return_value = (5,)

    # Clear all
    count = manager.clear_all()

    # Verify queries with fully qualified table names (skip table creation calls)
    count_calls = [
        call for call in mock_cursor.execute.call_args_list if "SELECT COUNT(*)" in call[0][0]
    ]
    truncate_calls = [
        call for call in mock_cursor.execute.call_args_list if "TRUNCATE TABLE" in call[0][0]
    ]

    assert len(count_calls) == 1
    assert len(truncate_calls) == 1
    assert "SELECT COUNT(*) FROM testdb.testschema.meltano_state" in count_calls[0][0][0]
    assert "TRUNCATE TABLE testdb.testschema.meltano_state" in truncate_calls[0][0][0]

    # Verify returned count
    assert count == 5


def test_acquire_lock(
    subject: tuple[SnowflakeStateStoreManager, mock.Mock],
) -> None:
    """Test acquiring and releasing lock."""
    manager, mock_cursor = subject

    # Test successful lock acquisition
    with manager.acquire_lock("test_job", retry_seconds=0):
        # Verify INSERT query for lock with fully qualified table name (skip table creation calls)  # noqa: E501
        insert_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "INSERT INTO" in call[0][0] and "meltano_state_locks" in call[0][0]
        ]
        assert len(insert_calls) >= 1
        assert "INSERT INTO testdb.testschema.meltano_state_locks" in insert_calls[0][0][0]

    # Verify DELETE queries for lock release and cleanup with fully qualified table names  # noqa: E501
    delete_calls = [
        call
        for call in mock_cursor.execute.call_args_list
        if "DELETE FROM testdb.testschema.meltano_state_locks" in call[0][0]
    ]
    assert len(delete_calls) >= 1


def test_acquire_lock_retry(
    subject: tuple[SnowflakeStateStoreManager, mock.Mock],
) -> None:
    """Test lock retry mechanism."""
    manager, mock_cursor = subject

    # Create a mock exception that mimics ProgrammingError
    mock_programming_error = Exception("Duplicate key")

    # Mock lock conflict on first attempt, success on second
    mock_cursor.execute.side_effect = [
        mock_programming_error,  # First attempt fails
        None,  # Success on second attempt
        None,  # Lock release
        None,  # Lock cleanup
    ]

    # Mock the ProgrammingError class used in the implementation
    with (
        mock.patch("snowflake.connector.errors.ProgrammingError", Exception),
        manager.acquire_lock("test_job", retry_seconds=0.01),  # type: ignore[arg-type]
    ):
        pass

    # Verify it retried
    assert mock_cursor.execute.call_count >= 2


def test_missing_account_validation() -> None:
    """Test missing account validation."""
    with pytest.raises(
        MissingStateBackendSettingsError,
        match="Snowflake account is required",
    ):
        SnowflakeStateStoreManager(
            uri="snowflake://user:pass@/db",  # No account in hostname
            user="test",
            password="pass",  # noqa: S106
            warehouse="warehouse",
            database="db",
        )


def test_missing_user_validation() -> None:
    """Test missing user validation."""
    with pytest.raises(
        MissingStateBackendSettingsError,
        match="Snowflake user is required",
    ):
        SnowflakeStateStoreManager(
            uri="snowflake://account/db",  # No user in URI
            account="account",
            password="pass",  # noqa: S106
            warehouse="warehouse",
            database="db",
        )


def test_missing_password_validation() -> None:
    """Test missing password validation."""
    with pytest.raises(
        MissingStateBackendSettingsError,
        match="Snowflake password is required",
    ):
        SnowflakeStateStoreManager(
            uri="snowflake://user@account/db",  # No password in URI
            account="account",
            user="test",
            warehouse="warehouse",
            database="db",
        )


def test_missing_warehouse_validation() -> None:
    """Test missing warehouse validation."""
    with pytest.raises(
        MissingStateBackendSettingsError,
        match="Snowflake warehouse is required",
    ):
        SnowflakeStateStoreManager(
            uri="snowflake://user:pass@account/db",
            account="account",
            user="test",
            password="pass",  # noqa: S106
            database="db",
            # No warehouse parameter
        )


def test_missing_database_validation() -> None:
    """Test missing database validation."""
    with pytest.raises(
        MissingStateBackendSettingsError,
        match="Snowflake database is required",
    ):
        SnowflakeStateStoreManager(
            uri="snowflake://user:pass@account/",  # No database in path
            account="account",
            user="test",
            password="pass",  # noqa: S106
            warehouse="warehouse",
            # No database parameter
        )


def test_connection_with_role() -> None:
    """Test connection creation with role."""
    # Mock snowflake.connector.connect directly during manager creation
    with mock.patch("snowflake.connector.connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()
        mock_cursor_context = mock.Mock()
        mock_cursor_context.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_cursor_context.__exit__ = mock.Mock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor_context
        mock_connect.return_value = mock_conn

        manager = SnowflakeStateStoreManager(
            uri="snowflake://testuser:testpass@testaccount/testdb/testschema",
            account="testaccount",
            user="testuser",
            password="testpass",  # noqa: S106
            warehouse="testwarehouse",
            database="testdb",
            schema="testschema",
            role="testrole",
        )

        # Access the connection property to trigger the connection
        _ = manager.connection

        # Verify role was included in connection params
        mock_connect.assert_called_with(
            account="testaccount",
            user="testuser",
            password="testpass",  # noqa: S106
            warehouse="testwarehouse",
            database="testdb",
            schema="testschema",
            role="testrole",
        )


def test_acquire_lock_max_retries_exceeded(
    subject: tuple[SnowflakeStateStoreManager, mock.Mock],
) -> None:
    """Test lock acquisition with max retries exceeded."""
    manager, mock_cursor = subject

    # Create a mock exception that mimics ProgrammingError with duplicate key
    mock_programming_error = Exception("Duplicate key")

    # Mock lock conflict on all attempts
    mock_cursor.execute.side_effect = mock_programming_error

    retry_seconds = Decimal("0.01")

    # Mock the ProgrammingError class used in the implementation
    with (  # noqa: SIM117
        mock.patch("snowflake.connector.errors.ProgrammingError", Exception),
        mock.patch("meltano_state_backend_snowflake.backend.sleep") as mock_sleep,
        pytest.raises(
            StateIDLockedError,
            match="Could not acquire lock for state_id: test_job",
        ),
    ):
        with manager.acquire_lock("test_job", retry_seconds=retry_seconds):  # type: ignore[arg-type]
            pass

    assert mock_sleep.call_count == int(30 / retry_seconds) - 1


def test_acquire_lock_other_programming_error(
    subject: tuple[SnowflakeStateStoreManager, mock.Mock],
) -> None:
    """Test lock acquisition with non-duplicate key ProgrammingError."""
    manager, mock_cursor = subject

    # Create a mock exception that mimics ProgrammingError but not duplicate key
    mock_programming_error = Exception("Some other error")

    # Mock lock conflict on first attempt
    mock_cursor.execute.side_effect = mock_programming_error

    # Mock the ProgrammingError class used in the implementation
    with (  # noqa: SIM117
        mock.patch("snowflake.connector.errors.ProgrammingError", Exception),
        pytest.raises(Exception, match="Some other error"),
        mock.patch("meltano_state_backend_snowflake.backend.sleep") as mock_sleep,
    ):
        with manager.acquire_lock("test_job", retry_seconds=0.01):  # type: ignore[arg-type]
            pass

    mock_sleep.assert_not_called()


def test_acquire_lock_multiple_retries_then_success(
    subject: tuple[SnowflakeStateStoreManager, mock.Mock],
) -> None:
    """Test lock acquisition with multiple retries before success."""
    manager, mock_cursor = subject

    # Create a mock exception that mimics ProgrammingError with duplicate key
    mock_programming_error = Exception("Duplicate key")

    # Mock lock conflict on multiple attempts, then success
    mock_cursor.execute.side_effect = [
        mock_programming_error,  # First attempt fails
        mock_programming_error,  # Second attempt fails
        mock_programming_error,  # Third attempt fails
        None,  # Fourth attempt succeeds
        None,  # Lock release
        None,  # Lock cleanup
    ]

    # Mock the ProgrammingError class and sleep function
    with (
        mock.patch("snowflake.connector.errors.ProgrammingError", Exception),
        mock.patch("meltano_state_backend_snowflake.backend.sleep") as mock_sleep,
        manager.acquire_lock("test_job", retry_seconds=0.01),  # type: ignore[arg-type]
    ):
        # Verify sleep was called 3 times (for the 3 failed attempts)
        assert mock_sleep.call_count == 3
        mock_sleep.assert_called_with(0.01)
