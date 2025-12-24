import warnings

# Backwards-compat shim: `utils` was renamed to `mysql_postgres_mapping`.
warnings.warn(
    "The 'utils' module has been renamed to 'mysql_postgres_mapping'. "
    "Import from 'mysql_postgres_mapping' instead.",
    DeprecationWarning,
    stacklevel=2,
)

from mysql_postgres_mapping import *  # noqa: F401,F403
