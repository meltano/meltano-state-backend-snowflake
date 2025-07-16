# `meltano-state-backend-snowflake`

[![PyPI version](https://img.shields.io/pypi/v/meltano-state-backend-snowflake.svg?logo=pypi&logoColor=FFE873&color=blue)](https://pypi.org/project/meltano-state-backend-snowflake)
[![Python versions](https://img.shields.io/pypi/pyversions/meltano-state-backend-snowflake.svg?logo=python&logoColor=FFE873)](https://pypi.org/project/meltano-state-backend-snowflake)

This is a [Meltano][meltano] extension that provides a [Snowflake][snowflake] [state backend][state-backend].

## Installation

This package needs to be installed in the same Python environment as Meltano.

### From GitHub

#### With [uv]

```bash
uv tool install --with meltano-state-backend-snowflake meltano
```

#### With [pipx]

```bash
pipx install meltano
pipx inject meltano 'meltano-state-backend-snowflake
```

## Configuration

To store state in Snowflake, set the `state_backend.uri` setting to `snowflake://<user>:<password>@<account>/<database>/<schema>`.

State will be stored in two tables that Meltano will create automatically:
- `meltano_state` - Stores the actual state data
- `meltano_state_locks` - Manages concurrency locks

To authenticate to Snowflake, you'll need to provide:

```yaml
state_backend:
  uri: snowflake://my_user:my_password@my_account/my_database/my_schema
  snowflake:
    warehouse: my_warehouse  # Required: The compute warehouse to use
    role: my_role           # Optional: The role to use for the connection
```

Alternatively, you can provide credentials via individual settings:

```yaml
state_backend:
  uri: snowflake://my_account/my_database/my_schema
  snowflake:
    account: my_account
    user: my_user
    password: my_password
    warehouse: my_warehouse
    database: my_database
    schema: my_schema      # Defaults to PUBLIC if not specified
    role: my_role          # Optional
```

#### Connection Parameters

- **account**: Your Snowflake account identifier (e.g., `myorg-account123`)
- **user**: The username for authentication
- **password**: The password for authentication
- **warehouse**: The compute warehouse to use (required)
- **database**: The database where state will be stored
- **schema**: The schema where state tables will be created (defaults to PUBLIC)
- **role**: Optional role to use for the connection

#### Security Considerations

When storing credentials:
- Use environment variables for sensitive values in production
- Consider using Snowflake key-pair authentication (future enhancement)
- Ensure the user has CREATE TABLE, INSERT, UPDATE, DELETE, and SELECT privileges

Example using environment variables:

```bash
export MELTANO_STATE_BACKEND_SNOWFLAKE_PASSWORD='my_secure_password'
meltano config meltano set state_backend.uri 'snowflake://my_user@my_account/my_database'
meltano config meltano set state_backend.snowflake.warehouse 'my_warehouse'
```

## Development

### Setup

```bash
uv sync
```

### Run tests

Run all tests, type checks, linting, and coverage:

```bash
uvx -with tox-uv tox run-parallel
```

### Bump the version

```bash
uv version --bump <type>
```

[meltano]: https://meltano.com
[snowflake]: https://www.snowflake.com/
[state-backend]: https://docs.meltano.com/concepts/state_backends
[pipx]: https://github.com/pypa/pipx
[uv]: https://docs.astral.sh/uv
