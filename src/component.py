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
    streaming_export: Optional[bool] = Field(default=True, description="Use streaming export for large files")


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
        self.streaming_export = config.streaming_export
        self.duck = self.__init_duckdb()

    def __init_duckdb(self) -> DuckDBPyConnection:
        os.makedirs(DUCK_DB_DIR, exist_ok=True)

        # Optimizations according to DuckDB Performance Guide
        config = {
            "temp_directory": DUCK_DB_DIR,
            "threads": "1",
            "max_memory": self.memory_limit,
            "preserve_insertion_order": False,
            "enable_external_access": True,
            "force_compression": "uncompressed",
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

        read_params = []
        if self.mode in ("fill", "strict"):
            read_params.append("union_by_name=true")

        read_params_str = ", ".join(read_params) if read_params else ""
        read_params_str = f", {read_params_str}" if read_params_str else ""

        selected_columns = ", ".join(self.columns) if self.columns else "*"
        selected_columns += ", filename" if self.include_filename and selected_columns != "*" else ""

        # Optimized query with out-of-core processing
        # Use streaming approach without staging table
        if self.streaming_export:
            if self.include_filename:
                # Use staging table for filename processing
                stage_query = f"""
                CREATE OR REPLACE TABLE stage AS
                SELECT {selected_columns}
                FROM read_parquet('{parquet_glob}', filename=True{read_params_str})
                """

                if self.debug:
                    logging.info(f"Executing staging query for filename processing: {stage_query}")

                self.duck.execute(stage_query)

                # Process filename column
                self.duck.execute("""
                    ALTER TABLE stage ADD COLUMN parquet_filename TEXT;
                """)
                self.duck.execute("""
                    UPDATE stage
                    SET parquet_filename = concat('/', regexp_replace(filename, '^.*[\\\\/]', ''));
                """)
                self.duck.execute("ALTER TABLE stage DROP COLUMN IF EXISTS filename;")

                # Export with processed filename
                copy_query = f"""
                COPY (
                    SELECT * FROM stage
                ) TO '{table_path}' (
                    HEADER FALSE,
                    DELIMITER ',',
                    FORMAT CSV
                )
                """

                self.duck.execute(copy_query)

                # For manifest - get schema BEFORE dropping table
                schema_query = "DESCRIBE stage;"

                # Build manifest BEFORE cleanup
                table_meta = self.duck.execute(schema_query).fetchall()
                schema = OrderedDict(
                    {c[0]: ColumnDefinition(data_types=self._convert_dtypes(c[1])) for c in table_meta}
                )

                out_table = self.create_out_table_definition(
                    self.table_name,
                    schema=schema,
                    primary_key=self.primary_keys,
                    incremental=self.incremental,
                    has_header=False,
                )

                self.write_manifest(out_table)

                # Cleanup AFTER manifest is written
                self.duck.execute("DROP TABLE IF EXISTS stage")
            else:
                # Direct streaming export without filename
                copy_query = f"""
                COPY (
                    SELECT {selected_columns}
                    FROM read_parquet('{parquet_glob}', filename=True{read_params_str})
                ) TO '{table_path}' (
                    HEADER FALSE,
                    DELIMITER ',',
                    FORMAT CSV
                )
                """

                if self.debug:
                    logging.info("Executing streaming export query")

                self.duck.execute(copy_query)

                # Sampling for manifest
                schema_query = f"""
                DESCRIBE (
                    SELECT {selected_columns}
                    FROM read_parquet('{parquet_glob}', filename=True{read_params_str})
                    LIMIT 1
                )
                """

                # Build manifest
                table_meta = self.duck.execute(schema_query).fetchall()
                schema = OrderedDict(
                    {c[0]: ColumnDefinition(data_types=self._convert_dtypes(c[1])) for c in table_meta}
                )

                out_table = self.create_out_table_definition(
                    self.table_name,
                    schema=schema,
                    primary_key=self.primary_keys,
                    incremental=self.incremental,
                    has_header=False,
                )

                self.write_manifest(out_table)
        else:
            # Fallback to staging table for smaller files
            stage_query = f"""
            CREATE OR REPLACE TABLE stage AS
            SELECT {selected_columns}
            FROM read_parquet('{parquet_glob}', filename=True{read_params_str})
            """

            if self.debug:
                logging.info(f"Executing staging query: {stage_query}")

            self.duck.execute(stage_query)

            # Process filename column
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

            # Export to CSV
            copy_query = f"COPY stage TO '{table_path}' (HEADER FALSE, DELIMITER ',')"
            self.duck.execute(copy_query)

            # For manifest - get schema BEFORE dropping table
            schema_query = "DESCRIBE stage;"

            # Build manifest BEFORE cleanup
            table_meta = self.duck.execute(schema_query).fetchall()
            schema = OrderedDict({c[0]: ColumnDefinition(data_types=self._convert_dtypes(c[1])) for c in table_meta})

            out_table = self.create_out_table_definition(
                self.table_name,
                schema=schema,
                primary_key=self.primary_keys,
                incremental=self.incremental,
                has_header=False,
            )

            self.write_manifest(out_table)

            # Cleanup AFTER manifest is written
            self.duck.execute("DROP TABLE IF EXISTS stage")

    def process_large_files(self):
        """Specializovaná metoda pro velmi velké soubory s chunked processing"""
        table_path = os.path.join(self.tables_out_path, self.table_name)
        os.makedirs(os.path.dirname(table_path), exist_ok=True)

        parquet_glob = os.path.join(self.files_in_path, "**", self.file_mask)

        read_params = []
        if self.mode in ("fill", "strict"):
            read_params.append("union_by_name=true")

        read_params_str = ", ".join(read_params) if read_params else ""
        read_params_str = f", {read_params_str}" if read_params_str else ""

        selected_columns = ", ".join(self.columns) if self.columns else "*"
        selected_columns += ", filename" if self.include_filename and selected_columns != "*" else ""

        # Use chunked processing according to DuckDB Performance Guide
        # Get total number of rows
        count_query = f"""
        SELECT COUNT(*) as total_rows
        FROM read_parquet('{parquet_glob}', filename=True{read_params_str})
        """
        total_rows = self.duck.execute(count_query).fetchone()[0]

        if self.debug:
            logging.info(f"Total rows to process: {total_rows}")

        # For very large files use chunked processing
        chunk_size = 100000  # Optimized according to DuckDB recommendations
        offset = 0
        chunk_number = 0

        while offset < total_rows:
            chunk_query = f"""
            COPY (
                SELECT {selected_columns}
                FROM read_parquet('{parquet_glob}', filename=True{read_params_str})
                LIMIT {chunk_size} OFFSET {offset}
            ) TO '{table_path}.part{chunk_number:04d}' (
                HEADER FALSE,
                DELIMITER ',',
                FORMAT CSV
            )
            """

            if self.debug:
                logging.info(
                    f"Processing chunk {chunk_number + 1}: rows {offset + 1} to {min(offset + chunk_size, total_rows)}"
                )

            self.duck.execute(chunk_query)

            offset += chunk_size
            chunk_number += 1

        # Join all parts
        if chunk_number > 1:
            self.duck.execute(f"""
            COPY (
                SELECT * FROM read_csv_auto('{table_path}.part*')
            ) TO '{table_path}' (
                HEADER FALSE,
                DELIMITER ',',
                FORMAT CSV
            )
            """)

            # Cleanup temp files
            for i in range(chunk_number):
                temp_file = f"{table_path}.part{i:04d}"
                if os.path.exists(temp_file):
                    os.remove(temp_file)

        # Build manifest
        schema_query = f"DESCRIBE (SELECT * FROM read_csv_auto('{table_path}') LIMIT 1)"
        table_meta = self.duck.execute(schema_query).fetchall()
        schema = OrderedDict({c[0]: ColumnDefinition(data_types=self._convert_dtypes(c[1])) for c in table_meta})

        out_table = self.create_out_table_definition(
            self.table_name,
            schema=schema,
            primary_key=self.primary_keys,
            incremental=self.incremental,
            has_header=False,
        )

        self.write_manifest(out_table)

    def run(self):
        # Automatically select method based on file size
        if self.streaming_export:
            # Try streaming first
            try:
                self.process()
            except Exception as e:
                if "Out of Memory" in str(e):
                    if self.debug:
                        logging.info("Streaming failed, trying chunked processing")
                    self.process_large_files()
                else:
                    raise
        else:
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
