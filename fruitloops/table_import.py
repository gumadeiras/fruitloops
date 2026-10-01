"""Import CSV, TSV, Parquet, and Feather files into DuckDB tables."""

from __future__ import annotations

from pathlib import Path

from .duckdb_store import DEFAULT_DUCKDB_PATH, require_duckdb, safe_identifier, table_exists, table_row_count
from .setup_state import file_fingerprint, setup_state_matches, write_setup_state

def import_to_duckdb(
    path: Path,
    table_name: str,
    store: Path = DEFAULT_DUCKDB_PATH,
    replace: bool = False,
    skip_current: bool = False,
) -> dict[str, str]:
    duckdb = require_duckdb("import/query")
    store.parent.mkdir(parents=True, exist_ok=True)
    table_name = safe_identifier(table_name)
    stage_key = f"import:{table_name}"
    fingerprint = file_fingerprint(path)
    with duckdb.connect(str(store)) as connection:
        if skip_current and table_exists(connection, table_name) and setup_state_matches(
            connection,
            stage_key,
            fingerprint,
        ):
            return {
                "store": str(store),
                "table": table_name,
                "rows": table_row_count(connection, table_name),
                "status": "current",
            }
        if table_exists(connection, table_name) and not replace:
            return {
                "store": str(store),
                "table": table_name,
                "rows": table_row_count(connection, table_name),
                "status": "existing",
            }
        if replace:
            connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        if path.suffix == ".csv":
            connection.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto(?)",
                [str(path)],
            )
        elif path.suffix == ".tsv":
            connection.execute(
                f"CREATE TABLE {table_name} AS "
                "SELECT * FROM read_csv_auto(?, delim = '\t', header = true, sample_size = -1)",
                [str(path)],
            )
        elif path.suffix == ".parquet":
            connection.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet(?)",
                [str(path)],
            )
        elif path.suffix == ".feather":
            import_feather(connection, path, table_name)
        else:
            raise ValueError(f"unsupported import format: {path.suffix}")
        rows = connection.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        write_setup_state(connection, stage_key, fingerprint, str(rows))
    return {"store": str(store), "table": table_name, "rows": str(rows), "status": "imported"}


def import_feather(connection, path: Path, table_name: str) -> None:
    try:
        import pyarrow as pa
        import pyarrow.ipc as ipc
    except ImportError as exc:
        raise SystemExit(
            "Feather import requires pyarrow. Install or reinstall fruitloops to include runtime dependencies."
        ) from exc
    with pa.memory_map(str(path), "r") as source:
        reader = ipc.open_file(source)
        for index in range(reader.num_record_batches):
            table = pa.Table.from_batches([reader.get_batch(index)])
            connection.register("_fruitloops_arrow_import", table)
            if index == 0:
                connection.execute(
                    f"CREATE TABLE {table_name} AS SELECT * FROM _fruitloops_arrow_import"
                )
            else:
                connection.execute(f"INSERT INTO {table_name} SELECT * FROM _fruitloops_arrow_import")
            connection.unregister("_fruitloops_arrow_import")
