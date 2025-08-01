from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
import os
import sys

# Alembic Config object
config = context.config

# Set up Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Add project path to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import metadata
from models.base import Base

target_metadata = Base.metadata

# Ignore certain Airflow/PostGIS-related tables
def include_name_filter(obj, name, type_, reflected, compare_to):
    ignored_tables = {
        # Airflow tables
        "dataset_event", "dagrun_dataset_event",
        "dag_run", "task_instance",

        # PostGIS / Tiger extension tables
        "spatial_ref_sys", "bg", "zip_state",
        "addr", "edges", "faces", "featnames", "geocode_settings", "geocoder_settings",
        "loader_lookuptables", "loader_platform", "pagc_gaz", "pagc_lex", "pagc_rules",
        "place", "secondary_unit", "state", "street_type", "tabblock", "tract", "zcta5",
        "place_lookup", "tabblock20",

        # Flask AppBuilder tables
        "ab_role", "ab_user", "ab_user_role",
        "ab_permission", "ab_view_menu", "ab_permission_view",
        "ab_register_user", "ab_permission_view_role",
        "ab_user_activity", "ab_user_stats", "ab_user_log",
    }

    # Ignore system schemas or extensions too
    if name in ignored_tables:
        return False
    if name and (
        name.startswith("tiger.") or
        name.startswith("topology.") or
        name.startswith("pg_") or
        name.startswith("sqlalchemy_")
    ):
        return False
    return True

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        include_object=include_name_filter,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_name_filter,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
