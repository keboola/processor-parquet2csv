import os
import logging
from typing import List, Optional
from pydantic import BaseModel, Field

from keboola.component import ComponentBase, UserException
from keboola.component.dao import BaseType, ColumnDefinition
from duckdb import connect, DuckDBPyConnection


class ComponentConfig(BaseModel):
    table_name: str = Field(..., description="Name of the output table")
    columns: List[str] = Field(default_factory=list, description="List of columns to include")
    incremental: bool = Field(default=False, description="Whether to run in incremental mode")
    primary_keys: List[str] = Field(..., description="List of primary key columns")
    include_filename: bool = Field(default=False, description="Whether to include filename column")
    file_mask: str = Field(default="*.parquet", description="File mask for parquet files")
    debug: bool = Field(default=False, description="Enable debug logging")
    fill_empty_values: bool = Field(default=False, description="Fill empty values with defaults")
    mode: Optional[str] = Field(default="fast", description="Mode for backward compatibility")


KEY_FILENAME_COLUMN = "parquet_filename"

DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")
DUCK_DB_MAX_MEMORY = "128MB"


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        params = self.configuration.parameters
        config = ComponentConfig(**params)

        self.table_name = config.table_name
        self.columns = config.columns
        self.incremental = config.incremental
        self.primary_keys = config.primary_keys
        self.include_filename = config.include_filename
        self.file_mask = config.file_mask
        self.debug = config.debug
        self.fill_empty_values = config.fill_empty_values or (config.mode == "fill")
        self.mode = config.mode
        self.duck = self.__init_duckdb()

    @staticmethod
    def __init_duckdb() -> DuckDBPyConnection:
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        config = {
            "temp_directory": DUCK_DB_DIR,
            "threads": "1",
            "max_memory": DUCK_DB_MAX_MEMORY,
        }
        return connect(config=config)

    def _convert_dtypes(self, dtype) -> BaseType:
        dtype = str(dtype).upper()

        if dtype == "NUMBER":
            return BaseType.float()

        if dtype in [
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "UHUGEINT",
        ]:
            return BaseType.integer()

        if dtype in ["REAL", "DECIMAL"]:
            return BaseType.numeric()

        if dtype == "DOUBLE":
            return BaseType.float()

        if dtype in ["BOOLEAN", "BOOL"]:
            return BaseType.boolean()

        if dtype in [
            "TIMESTAMP",
            "TIMESTAMP_NS",
            "TIMESTAMP WITH TIME ZONE",
            "DATETIME",
        ]:
            return BaseType.timestamp()

        if dtype == "DATE":
            return BaseType.date()

        if dtype in ["VARCHAR", "STRING", "CHAR", "TEXT"]:
            return BaseType.string()

        return BaseType.string()

    def _get_default_value(self, col_name):
        """Get default value based on column name"""
        col = col_name.upper()
        if col == "INTEGER":
            return "0"
        elif col in ["NUMERIC", "FLOAT"]:
            return "0.0"
        elif col == "BOOLEAN":
            return "'false'"
        elif col in ["DATE", "TIMESTAMP"]:
            return "NULL"
        else:
            return "''"

    def _get_default_value_by_type(self, dtype):
        dtype = str(dtype).upper()
        if dtype in [
            "INTEGER",
            "TINYINT",
            "SMALLINT",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "UHUGEINT",
        ]:
            return "0"
        if dtype in ["REAL", "DECIMAL", "FLOAT", "DOUBLE", "NUMERIC"]:
            return "0.0"
        if dtype in ["BOOLEAN", "BOOL"]:
            return "'false'"
        if dtype in ["DATE", "TIMESTAMP", "TIMESTAMP_NS", "TIMESTAMP WITH TIME ZONE", "DATETIME"]:
            return "NULL"
        return "''"

    def process(self):
        table_path = os.path.join(self.tables_out_path, self.table_name)
        os.makedirs(os.path.dirname(table_path), exist_ok=True)

        parquet_glob = os.path.join(self.files_in_path, "**", self.file_mask)
        # Only set filename=True if needed
        filename_param = ", filename=True" if self.include_filename else ""
        files_query = (
            f"SELECT DISTINCT filename FROM read_parquet('{parquet_glob}'{filename_param}) WHERE length(filename) > 0"
        )
        all_files = [row[0] for row in self.duck.execute(files_query).fetchall()]
        if not all_files:
            raise UserException("No parquet files found.")

        mode = self.mode or "fast"

        # Get columns and schema based on mode
        if mode == "fast":
            # Fast mode: Use schema from first file only
            if self.columns:
                columns = self.columns
                schema = {col: "STRING" for col in columns}
            else:
                self.duck.execute(f"CREATE TABLE base_table AS SELECT * FROM read_parquet('{all_files[0]}') LIMIT 0")
                schema = {row[0]: row[1] for row in self.duck.execute("DESCRIBE base_table").fetchall()}
                self.duck.execute("DROP TABLE base_table")
                columns = list(schema.keys())
        elif mode == "fill":
            # Fill mode: Union schemas from all files
            unioned_schema = {}
            for file in all_files:
                self.duck.execute(f"CREATE TABLE temp_schema AS SELECT * FROM read_parquet('{file}') LIMIT 0")
                for name, dtype, *_ in self.duck.execute("DESCRIBE temp_schema").fetchall():
                    if name not in unioned_schema:
                        unioned_schema[name] = dtype
                self.duck.execute("DROP TABLE temp_schema")

            if self.columns:
                columns = self.columns
            else:
                columns = list(unioned_schema.keys())
            schema = {col: unioned_schema.get(col, "STRING") for col in columns}
        else:  # strict mode
            # Strict mode: Must have specified columns
            if not self.columns:
                raise UserException("Columns must be specified in strict mode")
            columns = self.columns

            # Check all files have required columns
            for file in all_files:
                self.duck.execute(f"CREATE TABLE temp_schema AS SELECT * FROM read_parquet('{file}') LIMIT 0")
                file_columns = {row[0] for row in self.duck.execute("DESCRIBE temp_schema").fetchall()}
                self.duck.execute("DROP TABLE temp_schema")
                missing = set(columns) - file_columns
                if missing:
                    raise UserException(f"File '{file}' is missing required columns: {sorted(missing)}")

            # Get types from first file for consistency
            self.duck.execute(f"CREATE TABLE base_table AS SELECT * FROM read_parquet('{all_files[0]}') LIMIT 0")
            first_file_schema = {row[0]: row[1] for row in self.duck.execute("DESCRIBE base_table").fetchall()}
            self.duck.execute("DROP TABLE base_table")
            schema = {col: first_file_schema.get(col, "STRING") for col in columns}

        # --- Build SELECT columns with correct default values and order ---
        select_columns = []
        for col in columns:
            col_type = schema.get(col, "STRING")
            if mode == "fill" or self.fill_empty_values:
                default_val = self._get_default_value_by_type(col_type)
                select_columns.append(f'COALESCE("{col}", {default_val}) as "{col}"')
            else:
                select_columns.append(f'"{col}"')

        # Add filename column if needed
        if self.include_filename:
            select_columns.append(
                f"COALESCE(REPLACE('/' || regexp_replace(filename, '^.*[\\\\/]', ''), '\\\\', '/'), '') as {KEY_FILENAME_COLUMN}"  # noqa: E501
            )

        # Mode-specific DuckDB parameters
        union_by_name_param = ", union_by_name=True" if mode in ("fill", "strict") else ""

        # Build the query
        select_clause = ", ".join(select_columns)
        query = f"SELECT {select_clause} FROM read_parquet('{parquet_glob}'{filename_param}{union_by_name_param})"
        self.duck.execute(f"CREATE TABLE result_table AS {query}")

        self.duck.execute(f"""
            COPY result_table TO '{table_path}'
            (HEADER FALSE, DELIMITER ',', QUOTE '"')
        """)

        # Build manifest schema in the correct order
        schema_def = {}
        for col in columns:
            schema_def[col] = ColumnDefinition(
                data_types=self._convert_dtypes(schema.get(col, "STRING")),
                nullable=True,
                primary_key=(col in self.primary_keys),
            )

        # Only add filename column to schema if it was requested
        if self.include_filename:
            schema_def[KEY_FILENAME_COLUMN] = ColumnDefinition(
                data_types=BaseType.string(),
                nullable=True,
                primary_key=False,
            )

        out_table = self.create_out_table_definition(
            name=self.table_name, schema=schema_def, primary_key=self.primary_keys
        )
        self.write_manifest(out_table)

        self.duck.execute("DROP TABLE IF EXISTS result_table")

    def run(self):
        self.process()


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
