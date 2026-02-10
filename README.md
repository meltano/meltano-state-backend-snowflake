# `meltano-state-backend-snowflake`

<!-- Display these if and when we publish to PyPI. -->

<!--
[![PyPI version](https://img.shields.io/pypi/v/meltano-state-backend-snowflake.svg?logo=pypi&logoColor=FFE873&color=blue)](https://pypi.org/project/meltano-state-backend-snowflake)
[![Python versions](https://img.shields.io/pypi/pyversions/meltano-state-backend-snowflake.svg?logo=python&logoColor=FFE873)](https://pypi.org/project/meltano-state-backend-snowflake) -->

This is a [Meltano] extension that provides a [Snowflake] [state backend][state-backend].

## Installation

This package needs to be installed in the same Python environment as Meltano.

### From GitHub

#### With [uv]

```bash
uv tool install --with git+https://github.com/meltano/meltano-state-backend-snowflake.git meltano
```

#### With [pipx]

```bash
pipx install meltano
pipx inject meltano git+https://github.com/meltano/meltano-state-backend-snowflake.git
```

## Configuration

To store state in Snowflake, set the `state_backend.uri` setting to a [Snowflake SQLAlchemy][snowflake-sqlalchemy]-style URI:

```
snowflake://<user>:<password>@<account>/<database>/<schema>?warehouse=<warehouse>&role=<role>
```

State will be stored in two tables that Meltano will create automatically:

- `meltano_state` - Stores the actual state data
- `meltano_state_locks` - Manages concurrency locks

All connection parameters can be provided in the URI (including as query parameters), as individual Meltano settings, or a mix of both. Explicit settings take precedence over URI values.

Using a single URI with query parameters:

```yaml
state_backend:
  uri: snowflake://my_user:my_password@my_account/my_database/my_schema?warehouse=my_warehouse&role=my_role
```

Using a URI with separate settings for warehouse and role:

```yaml
state_backend:
  uri: snowflake://my_user:my_password@my_account/my_database/my_schema
  snowflake:
    warehouse: my_warehouse  # Required: The compute warehouse to use
    role: my_role           # Optional: The role to use for the connection
```

Using individual settings for everything:

```yaml
state_backend:
  uri: snowflake://my_account
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
- **password**: The password for authentication (required unless using key pair authentication)
- **warehouse**: The compute warehouse to use (required)
- **database**: The database where state will be stored
- **schema**: The schema where state tables will be created (defaults to PUBLIC)
- **role**: Optional role to use for the connection
- **private_key_base64**: Optional base64-encoded DER private key for key pair authentication

#### Key Pair Authentication

Instead of password-based authentication, you can use [Snowflake key pair authentication][snowflake-keypair]. Provide the private key as a base64-encoded DER-format string:

```yaml
state_backend:
  uri: snowflake://my_user@my_account/my_database?warehouse=my_warehouse
  snowflake:
    private_key_base64: MIIEvgIBADANBg...  # base64-encoded DER private key
```

The private key can also be passed as a URI query parameter:

```
snowflake://my_user@my_account/my_database?warehouse=my_warehouse&private_key_base64=MIIEvgIBADANBg...
```

Or via an environment variable:

```bash
export MELTANO_STATE_BACKEND_SNOWFLAKE_PRIVATE_KEY_BASE64='MIIEvgIBADANBg...'
```

To generate the base64-encoded DER key from a PEM private key file:

```bash
openssl pkcs8 -topk8 -inform PEM -outform DER -in rsa_key.pem -nocrypt | base64
```

When using key pair authentication, no password is required.

#### Security Considerations

When storing credentials:

- Use environment variables for sensitive values in production
- Ensure the user has CREATE TABLE, INSERT, UPDATE, DELETE, and SELECT privileges

Example using environment variables:

```bash
export MELTANO_STATE_BACKEND_SNOWFLAKE_PASSWORD='my_secure_password'
meltano config set meltano state_backend.uri 'snowflake://my_user@my_account/my_database?warehouse=my_warehouse'
```

Passwords containing special characters (e.g. `@`, `%`) must be URL-encoded when included in the URI. For example, `p@ss` becomes `p%40ss`.

## Development

### Setup

```bash
uv sync
```

### Run tests

Run all tests, type checks, linting, and coverage:

```bash
uvx --with tox-uv tox run-parallel
```

### Bump the version

Using the [GitHub CLI][gh]:

```bash
gh release create v<new-version>
```

[gh]: https://cli.github.com/
[meltano]: https://meltano.com
[pipx]: https://github.com/pypa/pipx
[snowflake]: https://www.snowflake.com/
[snowflake-keypair]: https://docs.snowflake.com/en/user-guide/key-pair-auth
[snowflake-sqlalchemy]: https://github.com/snowflakedb/snowflake-sqlalchemy
[state-backend]: https://docs.meltano.com/concepts/state_backends
[uv]: https://docs.astral.sh/uv
