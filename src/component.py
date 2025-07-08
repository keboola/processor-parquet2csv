import os
import logging
from collections import OrderedDict
from typing import List, Optional
from pydantic import BaseModel, Field

from keboola.component import ComponentBase, UserException
from keboola.component.dao import BaseType, ColumnDefinition
from duckdb import connect, DuckDBPyConnection


class ComponentConfig(BaseModel):
    table_name: str = Field(..., description="Name of the output table")
    columns: List[str] = Field(default_factory=list, description="List of columns to include")
    incremental: bool = Field(default=False, description="Whether to run in incremental mode")
    primary_keys: List[str] = Field(default_factory=list, description="List of primary key columns")
    include_filename: bool = Field(default=False, description="Whether to include filename column")
    file_mask: str = Field(default="*.parquet", description="File mask for parquet files")
    debug: bool = Field(default=False, description="Enable debug logging")
    fill_empty_values: bool = Field(default=False, description="Fill empty values with defaults")
    mode: Optional[str] = Field(default="fast", description="Mode for backward compatibility")
    memory_limit: Optional[str] = Field(default="1024MB", description="Memory limit for DuckDB")
    preserve_insertion_order: Optional[bool] = Field(default=True, description="Preserve insertion order")


DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


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
        self.memory_limit = config.memory_limit
        self.preserve_insertion_order = config.preserve_insertion_order
        self.duck = self.__init_duckdb()

    def __init_duckdb(self) -> DuckDBPyConnection:
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        config = {
            "temp_directory": DUCK_DB_DIR,
            "threads": "1",
            "max_memory": self.memory_limit,
            "preserve_insertion_order": self.preserve_insertion_order,
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

    def process(self):
        table_path = os.path.join(self.tables_out_path, self.table_name)
        os.makedirs(os.path.dirname(table_path), exist_ok=True)

        parquet_glob = os.path.join(self.files_in_path, "**", self.file_mask)
        union_by_name_param = ", union_by_name=true" if self.mode in ("fill", "strict") else ""
        selected_columns = ", ".join(self.columns) if self.columns else "*"
        selected_columns += ", filename" if self.include_filename and selected_columns != "*" else ""

        stage_query = f"CREATE OR REPLACE TABLE stage AS SELECT {selected_columns} FROM read_parquet('{parquet_glob}', filename=True{union_by_name_param})"  # noqa: E501
        self.duck.execute(stage_query)

        if self.include_filename:
            self.duck.execute("""
                ALTER TABLE stage ADD COLUMN parquet_filename TEXT;
            """)
            self.duck.execute("""
                UPDATE stage
                SET parquet_filename = concat('/', regexp_replace(filename, '^.*[\\\\/]', ''));
            """)
            self.duck.execute("ALTER TABLE stage DROP COLUMN IF EXISTS filename;")
        else:
            self.duck.execute("ALTER TABLE stage DROP COLUMN IF EXISTS filename")

        self.duck.execute(f"COPY stage TO '{table_path}' (HEADER FALSE, DELIMITER ',')")

        # Build manifest
        table_meta = self.duck.execute("""DESCRIBE stage;""").fetchall()
        schema = OrderedDict({c[0]: ColumnDefinition(data_types=self._convert_dtypes(c[1])) for c in table_meta})

        out_table = self.create_out_table_definition(
            self.table_name,
            schema=schema,
            primary_key=self.primary_keys,
            incremental=self.incremental,
            has_header=False,
        )

        self.write_manifest(out_table)

        self.duck.execute("DROP TABLE IF EXISTS stage")

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
