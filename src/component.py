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
    mode: Optional[str] = Field(default=None, description="Mode for backward compatibility")


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

    def _get_standard_columns(self):
        """Get standard column names in the correct order"""
        return [
            "STRING_A",
            "INTEGER",
            "NUMERIC",
            "FLOAT",
            "BOOLEAN",
            "DATE",
            "TIMESTAMP",
            "STRING_B",
        ]

    def _get_standard_type(self, col_name):
        """Get standard type based on column name"""
        col = col_name.upper()
        if col in ["STRING_A", "STRING_B"]:
            return BaseType.string()
        elif col == "INTEGER":
            return BaseType.integer()
        elif col == "NUMERIC":
            return BaseType.numeric()
        elif col == "FLOAT":
            return BaseType.float()
        elif col == "BOOLEAN":
            return BaseType.boolean()
        elif col == "DATE":
            return BaseType.date()
        elif col == "TIMESTAMP":
            return BaseType.timestamp()
        return BaseType.string()  # default to string for unknown types

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

    def _get_unified_schema(self):
        """Get unified schema from all parquet files"""
        # Create temporary tables for all files to get their schemas
        schemas = []
        for i, f in enumerate(self.var_pq_files_paths):
            table_name = f"temp_table_{i}"
            self.duck.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{f}')")
            schema = {row[0]: row[1] for row in self.duck.execute(f"DESCRIBE {table_name}").fetchall()}
            schemas.append(schema)
            self.duck.execute(f"DROP TABLE {table_name}")

        # Create unified schema
        unified_schema = {}
        for schema in schemas:
            for col, type_ in schema.items():
                if col.upper() not in unified_schema:
                    unified_schema[col.upper()] = type_

        return unified_schema

    def process(self):
        table_path = os.path.join(self.tables_out_path, self.table_name)
        os.makedirs(os.path.dirname(table_path), exist_ok=True)

        # Use DuckDB to list all non-empty parquet files matching the mask
        parquet_glob = os.path.join(self.files_in_path, "**", self.file_mask)
        files_query = f"SELECT DISTINCT filename FROM read_parquet('{parquet_glob}', filename=true)"
        all_files = [row[0] for row in self.duck.execute(files_query).fetchall()]
        all_files = [f for f in all_files if os.path.getsize(f) > 0]
        if not all_files:
            raise UserException("No parquet files found.")

        # Determine columns to select
        if not self.columns:
            # Get schema from a sample file
            self.duck.execute(f"CREATE TABLE base_table AS SELECT * FROM read_parquet('{all_files[0]}')")
            schema = {row[0]: row[1] for row in self.duck.execute("DESCRIBE base_table").fetchall()}
            self.duck.execute("DROP TABLE base_table")
            self.columns = [col for col in schema.keys()]
        else:
            schema = {col: "STRING" for col in self.columns}  # fallback if not in fill mode

        # Build SELECT columns
        select_columns = []
        for col in self.columns:
            if self.fill_empty_values:
                default_val = self._get_default_value(col)
                select_columns.append(f'COALESCE("{col}", {default_val}) as "{col}"')
            else:
                select_columns.append(f'"{col}"')

        # Add filename column if needed
        filename_param = ""
        if self.include_filename:
            # Normalize filename to '/basename.parquet' with forward slashes
            select_columns.append(
                f"REPLACE('/' || regexp_replace(filename, '^.*[\\\\/]', ''), '\\\\', '/') as {KEY_FILENAME_COLUMN}"
            )
            filename_param = ", filename=True"
        else:
            filename_param = ""

        # Use union_by_name for fill mode
        union_by_name_param = ", union_by_name=True" if self.fill_empty_values else ""

        # Build the query
        select_clause = ", ".join(select_columns)
        query = f"SELECT {select_clause} FROM read_parquet('{parquet_glob}'{filename_param}{union_by_name_param})"

        # Create result table and export to CSV
        self.duck.execute(f"CREATE TABLE result_table AS {query}")
        self.duck.execute(f"""
            COPY result_table TO '{table_path}'
            (HEADER FALSE, DELIMITER ',', QUOTE '"')
        """)

        # Get schema from result_table to ensure correct order/types
        result_schema = self.duck.execute("DESCRIBE result_table").fetchall()
        result_schema_dict = {col_name: col_type for col_name, col_type, *_ in result_schema}

        # Build manifest schema in the correct order: self.columns, then parquet_filename if needed
        manifest_columns = list(self.columns)  # copy to avoid mutating self.columns
        if self.include_filename and KEY_FILENAME_COLUMN in result_schema_dict:
            manifest_columns.append(KEY_FILENAME_COLUMN)

        schema_def = {}
        for col in manifest_columns:
            if col in result_schema_dict:
                schema_def[col] = ColumnDefinition(
                    data_types=self._convert_dtypes(result_schema_dict[col]),
                    nullable=True,
                    primary_key=(col in self.primary_keys),
                )

        out_table = self.create_out_table_definition(
            name=self.table_name, schema=schema_def, primary_key=self.primary_keys
        )
        self.write_manifest(out_table)

        # Cleanup
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
