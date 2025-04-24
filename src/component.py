import os
import glob
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

    def get_parquet_files(self):
        all_parquet_files = glob.glob(
            os.path.join(self.files_in_path, "**", self.file_mask),
            recursive=True,
        )
        all_parquet_files.sort()

        if len(all_parquet_files) == 0:
            raise UserException("No parquet files found.")

        nonempty_parquet_files = [path for path in all_parquet_files if os.path.getsize(path) > 0]
        empty_parquet_files = [path for path in all_parquet_files if os.path.getsize(path) == 0]

        logging.info(f"Skipping {len(empty_parquet_files)} empty files.")

        self.var_pq_files_paths = nonempty_parquet_files
        self.var_pq_files_names = [x.replace(self.files_in_path, "") for x in nonempty_parquet_files]

        logging.debug(f"Processing {len(self.var_pq_files_names)} files. Names:\n{self.var_pq_files_names}.")

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

        # Get unified schema from all files
        if self.fill_empty_values:
            schema = self._get_unified_schema()
        else:
            # For fast mode, just use schema from first file
            self.duck.execute(f"CREATE TABLE base_table AS SELECT * FROM read_parquet('{self.var_pq_files_paths[0]}')")
            schema = {row[0]: row[1] for row in self.duck.execute("DESCRIBE base_table").fetchall()}
            self.duck.execute("DROP TABLE base_table")

        # If no columns specified, use all columns from schema
        if not self.columns:
            self.columns = [col for col in schema.keys()]

        # Build the main query
        query_parts = []

        # Process each file
        for f in self.var_pq_files_paths:
            select_columns = []
            for col in self.columns:
                # Find the actual column name in schema (case-insensitive)
                actual_col = next((k for k in schema.keys() if k.upper() == col.upper()), None)
                if actual_col:
                    if self.fill_empty_values:
                        default_val = self._get_default_value(col)
                        select_columns.append(f'COALESCE("{actual_col}", {default_val}) as "{col}"')
                    else:
                        select_columns.append(f'"{actual_col}" as "{col}"')
                else:
                    if self.fill_empty_values:
                        # For missing columns in fill mode, add default value
                        default_val = self._get_default_value(col)
                        select_columns.append(f'{default_val} as "{col}"')
                    else:
                        raise UserException(f"Column {col} not found in Parquet schema")

            if self.include_filename:
                filename = "/" + os.path.basename(f)
                select_columns.append(f"'{filename}' as {KEY_FILENAME_COLUMN}")

            query_parts.append(f"SELECT {', '.join(select_columns)} FROM read_parquet('{f}')")

        # Combine all parts with UNION ALL
        query = " UNION ALL ".join(query_parts)

        # Create result table and export to CSV
        self.duck.execute(f"CREATE TABLE result_table AS {query}")
        self.duck.execute(f"""
            COPY result_table TO '{table_path}'
            (HEADER FALSE, DELIMITER ',', QUOTE '"')
        """)

        # Create manifest with columns in the same order as in the query
        schema_def = {}
        for col in self.columns:
            # Get the actual column type from schema
            actual_col = next((k for k in schema.keys() if k.upper() == col.upper()), None)
            if actual_col:
                col_type = schema[actual_col]
            else:
                col_type = "STRING"  # Default type for missing columns

            schema_def[col] = ColumnDefinition(
                data_types=self._convert_dtypes(col_type),
                nullable=True,
                primary_key=(col in self.primary_keys),
            )

        if self.include_filename:
            schema_def[KEY_FILENAME_COLUMN] = ColumnDefinition(
                data_types=BaseType.string(), nullable=True, primary_key=False
            )

        out_table = self.create_out_table_definition(
            name=self.table_name, schema=schema_def, primary_key=self.primary_keys
        )
        self.write_manifest(out_table)

        # Cleanup
        self.duck.execute("DROP TABLE IF EXISTS result_table")

    def run(self):
        self.get_parquet_files()
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
