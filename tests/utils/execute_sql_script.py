from pathlib import Path

import sqlalchemy as sa


def execute_sql_script(engine, script_file):
    with engine.connect() as connection, connection.begin(), open(script_file) as file:
        sql_script = file.read()
        connection.execute(sa.text(sql_script))


def execute_create_schema_script(engine):
    scripts_path = Path(__file__).resolve().parents[1] / "database_scripts"
    execute_sql_script(engine, f"{scripts_path}/00_create_schema.sql")


def execute_insert_data_script(engine):
    scripts_path = Path(__file__).resolve().parents[1] / "database_scripts"
    execute_sql_script(engine, f"{scripts_path}/01_insert_data.sql")
