"""StateStoreManager for Snowflake state backend."""

from __future__ import annotations

import base64
import json
import typing as t
from contextlib import contextmanager
from functools import cached_property
from time import sleep
from urllib.parse import parse_qs, urlparse

import snowflake.connector
from meltano.core.error import MeltanoError
from meltano.core.setting_definition import SettingDefinition, SettingKind
from meltano.core.state_store.base import (
    MeltanoState,
    MissingStateBackendSettingsError,
    StateIDLockedError,
    StateStoreManager,
)

if t.TYPE_CHECKING:
    from collections.abc import Generator, Iterable


class SnowflakeStateBackendError(MeltanoError):
    """Base error for Snowflake state backend."""


SNOWFLAKE_ACCOUNT = SettingDefinition(
    name="state_backend.snowflake.account",
    label="Snowflake Account",
    description="Snowflake account identifier",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    env_specific=True,
)

SNOWFLAKE_USER = SettingDefinition(
    name="state_backend.snowflake.user",
    label="Snowflake User",
    description="Snowflake username",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    env_specific=True,
)

SNOWFLAKE_PASSWORD = SettingDefinition(
    name="state_backend.snowflake.password",
    label="Snowflake Password",
    description="Snowflake password",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    sensitive=True,
    env_specific=True,
)

SNOWFLAKE_WAREHOUSE = SettingDefinition(
    name="state_backend.snowflake.warehouse",
    label="Snowflake Warehouse",
    description="Snowflake compute warehouse",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    env_specific=True,
)

SNOWFLAKE_DATABASE = SettingDefinition(
    name="state_backend.snowflake.database",
    label="Snowflake Database",
    description="Snowflake database name",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    env_specific=True,
)

SNOWFLAKE_SCHEMA = SettingDefinition(
    name="state_backend.snowflake.schema",
    label="Snowflake Schema",
    description="Snowflake schema name",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    value="PUBLIC",
    env_specific=True,
)

SNOWFLAKE_ROLE = SettingDefinition(
    name="state_backend.snowflake.role",
    label="Snowflake Role",
    description="Snowflake role to use",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    env_specific=True,
)

SNOWFLAKE_PRIVATE_KEY_BASE64 = SettingDefinition(
    name="state_backend.snowflake.private_key_base64",
    label="Snowflake Private Key (Base64-encoded DER)",
    description="Snowflake private key to use, in base64-encoded DER format",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    sensitive=True,
    env_specific=True,
)


class SnowflakeStateStoreManager(StateStoreManager):
    """State backend for Snowflake."""

    label: str = "Snowflake"
    table_name: str = "meltano_state"
    lock_table_name: str = "meltano_state_locks"

    def __init__(
        self,
        uri: str,
        *,
        account: str | None = None,
        user: str | None = None,
        password: str | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        role: str | None = None,
        private_key_base64: str | None = None,
        **kwargs: t.Any,
    ) -> None:
        """Initialize the SnowflakeStateStoreManager.

        Args:
            uri: The state backend URI
            account: Snowflake account identifier
            user: Snowflake username
            password: Snowflake password
            warehouse: Snowflake compute warehouse
            database: Snowflake database name
            schema: Snowflake schema name (default: PUBLIC)
            role: Optional Snowflake role to use
            private_key_base64: Optional Snowflake private key to use, in base64-encoded DER format
            kwargs: Additional keyword args to pass to parent

        """
        super().__init__(**kwargs)
        self.uri = uri
        parsed = urlparse(uri)
        query_params = parse_qs(parsed.query)

        # Extract connection details from URI and parameters
        self.account = account or parsed.hostname
        if not self.account:
            msg = "Snowflake account is required"
            raise MissingStateBackendSettingsError(msg)

        self.user = user or parsed.username
        if not self.user:
            msg = "Snowflake user is required"
            raise MissingStateBackendSettingsError(msg)

        self.password = password or parsed.password
        if not self.password and not private_key_base64 and not query_params.get("private_key_base64"):
            msg = "Snowflake password or private key is required"
            raise MissingStateBackendSettingsError(msg)

        self.warehouse = warehouse or query_params.get("warehouse", [None])[0]
        if not self.warehouse:
            msg = "Snowflake warehouse is required"
            raise MissingStateBackendSettingsError(msg)

        # Extract database from path
        path_parts = parsed.path.strip("/").split("/") if parsed.path else []
        self.database = database or (path_parts[0] if path_parts else None)
        if not self.database:
            msg = "Snowflake database is required"
            raise MissingStateBackendSettingsError(msg)

        self.schema = schema or (path_parts[1] if len(path_parts) > 1 else "PUBLIC")
        self.role = role or query_params.get("role", [None])[0]

        pkey_base64 = private_key_base64 or query_params.get("private_key_base64", [None])[0]
        self.private_key = self._load_private_key(pkey_base64) if pkey_base64 else None

        self._ensure_tables()

    def _load_private_key(self, pkey_base64: str) -> bytes:
        # Restore '+' chars that parse_qs decodes as spaces
        pkey_base64 = pkey_base64.replace(" ", "+")
        # Add padding if stripped (e.g. from URL query params)
        pkey_base64 += "=" * (-len(pkey_base64) % 4)
        return base64.b64decode(pkey_base64)

    @cached_property
    def connection(self) -> snowflake.connector.SnowflakeConnection:
        """Get a Snowflake connection.

        Returns:
            A Snowflake connection object.

        """
        conn_params = {
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
        }
        if self.role:
            conn_params["role"] = self.role

        if self.private_key:
            conn_params["private_key"] = self.private_key
        elif self.password:
            conn_params["password"] = self.password

        return snowflake.connector.connect(**conn_params)

    def _ensure_tables(self) -> None:
        """Ensure the state and lock tables exist."""
        with self.connection.cursor() as cursor:
            # Create state table
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.database}.{self.schema}.{self.table_name} (
                    state_id VARCHAR PRIMARY KEY,
                    state VARIANT
                )
                """,
            )

            # Create lock table
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.database}.{self.schema}.{self.lock_table_name} (
                    state_id VARCHAR PRIMARY KEY,
                    locked_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    lock_id VARCHAR
                )
                """,
            )

    def set(self, state: MeltanoState) -> None:
        """Set the job state for the given state_id.

        Args:
            state: the state to set.

        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                MERGE INTO {self.database}.{self.schema}.{self.table_name} AS target
                USING (SELECT %s AS state_id, PARSE_JSON(%s) AS state) AS source
                ON target.state_id = source.state_id
                WHEN MATCHED THEN
                    UPDATE SET state = source.state
                WHEN NOT MATCHED THEN
                    INSERT (state_id, state)
                    VALUES (source.state_id, source.state)
                """,  # noqa: S608
                (state.state_id, state.json()),
            )

    def get(self, state_id: str) -> MeltanoState | None:
        """Get the job state for the given state_id.

        Args:
            state_id: the name of the job to get state for.

        Returns:
            The current state for the given job

        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT state
                FROM {self.database}.{self.schema}.{self.table_name}
                WHERE state_id = %s
                """,  # noqa: S608
                (state_id,),
            )
            row = cursor.fetchone()

            if not row:
                return None

            state = row[0]

            if state is None:
                return MeltanoState(state_id=state_id)

            # VARIANT columns may come back as a dict or a JSON string
            if isinstance(state, dict):
                state = json.dumps(state)

            return MeltanoState.from_json(state_id, state)

    def delete(self, state_id: str) -> None:
        """Delete state for the given state_id.

        Args:
            state_id: the state_id to clear state for

        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {self.database}.{self.schema}.{self.table_name} WHERE state_id = %s",  # noqa: S608
                (state_id,),
            )

    def clear_all(self) -> int:
        """Clear all states.

        Returns:
            The number of states cleared from the store.

        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"SELECT COUNT(*) FROM {self.database}.{self.schema}.{self.table_name}",  # noqa: S608
            )
            count = cursor.fetchone()[0]  # type: ignore[index]
            cursor.execute(
                f"TRUNCATE TABLE {self.database}.{self.schema}.{self.table_name}",
            )
            return count  # type: ignore[no-any-return]

    def get_state_ids(self, pattern: str | None = None) -> Iterable[str]:
        """Get all state_ids available in this state store manager.

        Args:
            pattern: glob-style pattern to filter by

        Returns:
            An iterable of state_ids

        """
        with self.connection.cursor() as cursor:
            if pattern and pattern != "*":
                # Convert glob pattern to SQL LIKE pattern
                sql_pattern = pattern.replace("*", "%").replace("?", "_")
                cursor.execute(
                    f"SELECT state_id FROM {self.database}.{self.schema}.{self.table_name} WHERE state_id LIKE %s",  # noqa: E501, S608
                    (sql_pattern,),
                )
            else:
                cursor.execute(
                    f"SELECT state_id FROM {self.database}.{self.schema}.{self.table_name}",  # noqa: S608
                )

            for row in cursor:
                yield row[0]

    @contextmanager
    def acquire_lock(
        self,
        state_id: str,
        *,
        retry_seconds: int = 1,
    ) -> Generator[None, None, None]:
        """Acquire a lock for the given job's state.

        Args:
            state_id: the state_id to lock
            retry_seconds: the number of seconds to wait before retrying

        Yields:
            None

        Raises:
            StateIDLockedError: if the lock cannot be acquired

        """
        import uuid

        lock_id = str(uuid.uuid4())
        max_seconds = 30
        seconds_waited = 0

        while seconds_waited < max_seconds:  # pragma: no branch
            try:
                with self.connection.cursor() as cursor:
                    # Try to acquire lock
                    cursor.execute(
                        f"""
                        INSERT INTO {self.database}.{self.schema}.{self.lock_table_name} (state_id, lock_id)
                        VALUES (%s, %s)
                        """,  # noqa: E501, S608
                        (state_id, lock_id),
                    )
                    break
            except snowflake.connector.errors.ProgrammingError as e:
                # Check if it's a unique constraint violation
                if "Duplicate key" in str(e):
                    seconds_waited += retry_seconds
                    if seconds_waited >= max_seconds:  # Last attempt
                        msg = f"Could not acquire lock for state_id: {state_id}"
                        raise StateIDLockedError(msg) from e
                    sleep(retry_seconds)
                else:
                    raise

        try:
            yield
        finally:
            # Release the lock
            with self.connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    DELETE FROM {self.database}.{self.schema}.{self.lock_table_name}
                    WHERE state_id = %s AND lock_id = %s
                    """,  # noqa: S608
                    (state_id, lock_id),
                )

            # Clean up old locks (older than 5 minutes)
            with self.connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    DELETE FROM {self.database}.{self.schema}.{self.lock_table_name}
                    WHERE locked_at < DATEADD(minute, -5, CURRENT_TIMESTAMP())
                    """,  # noqa: S608
                )
