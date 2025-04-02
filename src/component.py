import glob
import logging
import os

from keboola.component import ComponentBase, UserException
from keboola.component.dao import BaseType, ColumnDefinition
from duckdb import connect, DuckDBPyConnection

KEY_MODE = "mode"
KEY_TABLE_COLUMNS = "columns"
KEY_TABLE_NAME = "table_name"
KEY_INCREMENTAL = "incremental"
KEY_PRIMARY_KEYS = "primary_keys"
KEY_FILENAME = "include_filename"
KEY_CHUNKSIZE = "chunk_size"
KEY_DEBUG = "debug"
KEY_EXTENSION_MASK = "file_mask"

MANDATORY_PARAMETERS = [KEY_MODE, KEY_TABLE_NAME]
SUPPORTED_MODES = ["fast", "fill", "strict"]
FILENAME_COLUMN = "parquet_filename"

DEFAULT_CHUNK_SIZE = 10000

DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")
DUCK_DB_MAX_MEMORY = "128MB"


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.cfg_params = self.configuration.parameters

        try:
            self.validate_configuration_parameters(MANDATORY_PARAMETERS)

        except ValueError as e:
            raise UserException(f"Missing mandatory fields {e} in configuration.")

        self.par_mode = self.cfg_params[KEY_MODE]
        self.par_table_name = self.cfg_params[KEY_TABLE_NAME]
        self.par_table_columns = self.cfg_params.get(KEY_TABLE_COLUMNS, [])
        self.par_incremental = bool(self.cfg_params.get(KEY_INCREMENTAL, False))
        self.par_primary_keys = self.cfg_params.get(KEY_PRIMARY_KEYS, [])
        self.par_include_filename = bool(self.cfg_params.get(KEY_FILENAME, False))
        self.par_chunk_size = self.cfg_params.get(KEY_CHUNKSIZE, None)
        self.par_debug = self.cfg_params.get(KEY_DEBUG, False)
        self.par_extension_mask = self.cfg_params.get(KEY_EXTENSION_MASK, "*.parquet")
        self.duck = self.init_duckdb()

        if self.par_debug is True:
            logging.getLogger().setLevel("DEBUG")
        else:
            pass

        self.validateParameters()

    @staticmethod
    def init_duckdb() -> DuckDBPyConnection:
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        config = dict(
            temp_directory=DUCK_DB_DIR, threads="1", max_memory=DUCK_DB_MAX_MEMORY
        )
        return connect(config=config)

    def validateParameters(self):
        if self.par_mode not in SUPPORTED_MODES:
            raise UserException(
                f"Unsupported mode {self.par_mode}. Supported modes are: {SUPPORTED_MODES}."
            )

        if not isinstance(self.par_table_name, str):
            raise UserException('Parameter "table_name" must be of type string.')

        elif self.par_table_name.strip() == "":
            raise UserException("No table name provided.")

        elif self.par_table_name.endswith(".csv") is False:
            self.par_table_name = self.par_table_name + ".csv"

        else:
            pass

        if not isinstance(self.par_table_columns, list):
            raise UserException('Parameter "columns" must be of type list.')

        elif len(self.par_table_columns) == 0:
            self.par_table_columns = None

        if not isinstance(self.par_primary_keys, list):
            raise UserException('Parameter "primary_keys" must be of type list.')

        if self.par_chunk_size is None:
            self.par_chunk_size = DEFAULT_CHUNK_SIZE

        else:
            try:
                _cs = int(self.par_chunk_size)

            except ValueError:
                raise UserException(
                    'Parameter "chunk_size" must be either an integer or "null".'
                )

            self.par_chunk_size = _cs if _cs > 0 else DEFAULT_CHUNK_SIZE

    def getParquetFiles(self):
        all_parquet_files = glob.glob(
            os.path.join(self.files_in_path, "**", self.par_extension_mask),
            recursive=True,
        )
        all_parquet_files.sort()

        if len(all_parquet_files) == 0:
            raise UserException("No parquet files found.")

        else:
            nonempty_parquet_files = [
                path for path in all_parquet_files if os.path.getsize(path) > 0
            ]
            empty_parquet_files = [
                path for path in all_parquet_files if os.path.getsize(path) == 0
            ]

            logging.info(f"Skipping {len(empty_parquet_files)} empty files.")

            self.var_pq_files_paths = nonempty_parquet_files
            self.var_pq_files_names = [
                x.replace(self.files_in_path, "") for x in nonempty_parquet_files
            ]

            logging.debug(
                f"Processing {len(self.var_pq_files_names)} files. Names:\n{self.var_pq_files_names}."
            )

    def processParquet(self):
        path_table = os.path.join(self.tables_out_path, self.par_table_name)

        if self.par_mode == "fast":
            _schema = self._fastProcess(path_table)

        elif self.par_mode == "fill":
            _schema = self._fillProcess(path_table)

        elif self.par_mode == "strict":
            _schema = self._strictProcess(path_table)

        else:
            raise UserException(f"Unsupported mode {self.par_mode}.")

        schema = {
            k: ColumnDefinition(data_types=self.convert_dtypes(v))
            for k, v in _schema.items()
        }
        out_table = self.create_out_table_definition(
            self.par_table_name, schema=schema, primary_key=self.par_primary_keys
        )
        self.write_manifest(out_table)

    def convert_dtypes(self, dtype) -> BaseType:
        """Convert DuckDB types to Keboola base types"""
        dtype = str(dtype).upper()

        # Handle DuckDB's NUMBER type which can be either INTEGER or FLOAT
        if dtype == "NUMBER":
            return BaseType.float()  # Change to INTEGER for NUMBER type

        # Handle integer types
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

        # Handle decimal types
        if dtype in ["REAL", "DECIMAL"]:
            return BaseType.numeric()

        # Handle floating point types
        if dtype == "DOUBLE":
            return BaseType.float()

        # Handle boolean types
        if dtype in ["BOOLEAN", "BOOL"]:
            return BaseType.boolean()

        # Handle timestamp types
        if dtype in [
            "TIMESTAMP",
            "TIMESTAMP_NS",
            "TIMESTAMP WITH TIME ZONE",
            "DATETIME",
        ]:
            return BaseType.timestamp()

        # Handle date types
        if dtype == "DATE":
            return BaseType.date()

        # Handle string types
        if dtype in ["VARCHAR", "STRING", "CHAR", "TEXT"]:
            return BaseType.string()

        # Default to string for unknown types
        return BaseType.string()

    def _get_coalesce_expr(self, col, col_type, filename):
        """Helper method to generate COALESCE expression based on column type"""
        if col == FILENAME_COLUMN:
            # Replace backslashes with forward slashes in filename
            safe_filename = filename.replace("\\", "/")
            return f"'{safe_filename}' AS {FILENAME_COLUMN}"

        logging.debug(f"Column {col} has type: {col_type}")
        col_type = str(col_type).upper()

        # Handle DuckDB's NUMBER type which can be either INTEGER or FLOAT
        if col_type == "NUMBER":
            return f"COALESCE(CAST({col} AS DOUBLE), 0) AS {col}"

        # Handle integer types
        if col_type in [
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
            return f"COALESCE(CAST({col} AS INTEGER), 0) AS {col}"

        # Handle decimal types
        if col_type in ["REAL", "DECIMAL"]:
            return f"COALESCE(CAST({col} AS DECIMAL), 0) AS {col}"

        # Handle floating point types
        if col_type == "DOUBLE":
            return f"COALESCE(CAST({col} AS DOUBLE), 0) AS {col}"

        # Handle boolean types
        if col_type in ["BOOLEAN", "BOOL"]:
            return f"COALESCE(CAST({col} AS BOOLEAN), false) AS {col}"

        # Handle timestamp types
        if col_type in [
            "TIMESTAMP",
            "TIMESTAMP_NS",
            "TIMESTAMP WITH TIME ZONE",
            "DATETIME",
        ]:
            return f"COALESCE(CAST({col} AS TIMESTAMP), NULL) AS {col}"

        # Handle date types
        if col_type == "DATE":
            return f"COALESCE(CAST({col} AS DATE), NULL) AS {col}"

        # Handle string types
        if col_type in ["VARCHAR", "STRING", "CHAR", "TEXT"]:
            return f"COALESCE(CAST({col} AS VARCHAR), '') AS {col}"

        # Default to string for unknown types
        return f"COALESCE(CAST({col} AS VARCHAR), '') AS {col}"

    def _processFastBatch(self, pq_path, temp_table, columns, filename):
        try:
            view_name = f"temp_view_{abs(hash(pq_path))}"
            self.duck.execute(f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * FROM parquet_scan('{pq_path}')
            """)

            if FILENAME_COLUMN in columns:
                self.duck.execute(f"""
                    CREATE OR REPLACE VIEW {view_name}_with_filename AS
                    SELECT *, '{filename}' AS {FILENAME_COLUMN} FROM {view_name}
                """)
                view_name = f"{view_name}_with_filename"

            # Get schema using DESCRIBE
            schema_info = self.duck.execute(f"DESCRIBE {view_name}").fetchall()
            schema = {col[0]: col[1] for col in schema_info}

            select_cols = [
                self._get_coalesce_expr(col, schema.get(col, ""), filename)
                for col in columns
            ]

            row_count = self.duck.execute(
                f"SELECT COUNT(*) FROM {view_name}"
            ).fetchone()[0]
            logging.info(f"Processing {filename} with {row_count} rows...")

            self.duck.execute(f"""
                INSERT INTO {temp_table}
                SELECT {", ".join(select_cols)}
                FROM {view_name}
            """)

            total_rows = self.duck.execute(
                f"SELECT COUNT(*) FROM {temp_table}"
            ).fetchone()[0]
            logging.info(
                f"Total rows in temporary table after {filename}: {total_rows}"
            )

        except Exception as e:
            logging.error(f"Error processing {pq_path}: {e}")
            raise

        finally:
            self.duck.execute(f"DROP VIEW IF EXISTS {view_name}_with_filename")
            self.duck.execute(f"DROP VIEW IF EXISTS {view_name}")

    def _create_temp_table(self, table_name, schema, columns):
        """Helper method to create temporary table with correct schema"""
        create_columns = []
        for col in columns:
            if col == FILENAME_COLUMN:
                create_columns.append(f"{col} VARCHAR")
            else:
                col_type = str(schema[col]).upper()
                if col_type == "NUMBER" or col_type == "NUMERIC":
                    create_columns.append(
                        f"{col} DECIMAL(18,4)"
                    )  # Adjust precision as needed
                elif (
                    col_type == "INTEGER"
                    or col_type == "BIGINT"
                    or col_type == "SMALLINT"
                ):
                    create_columns.append(f"{col} INTEGER")
                elif col_type == "DOUBLE" or col_type == "FLOAT" or col_type == "REAL":
                    create_columns.append(f"{col} DOUBLE")
                elif col_type == "BOOLEAN":
                    create_columns.append(f"{col} BOOLEAN")
                elif col_type == "DATE":
                    create_columns.append(f"{col} DATE")
                elif col_type == "TIMESTAMP" or col_type == "DATETIME":
                    create_columns.append(f"{col} TIMESTAMP")
                else:
                    create_columns.append(f"{col} VARCHAR")

        self.duck.execute(f"""
            CREATE TEMP TABLE {table_name} (
                {", ".join(create_columns)}
            )
        """)

    def _fastProcess(self, table_path):
        schema = None
        columns = None

        os.makedirs(os.path.dirname(table_path), exist_ok=True)

        first_path = self.var_pq_files_paths[0]
        # Create a temporary view to get schema using DESCRIBE
        view_name = f"temp_view_{abs(hash(first_path))}"
        self.duck.execute(f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM parquet_scan('{first_path}')
        """)

        # Get schema using DESCRIBE
        schema_info = self.duck.execute(f"DESCRIBE {view_name}").fetchall()
        schema = {col[0]: col[1] for col in schema_info}

        # Clean up the temporary view
        self.duck.execute(f"DROP VIEW IF EXISTS {view_name}")

        if not schema:
            raise UserException("Schema is empty.")

        if self.par_table_columns is None:
            columns = list(schema.keys())
        else:
            columns = self.par_table_columns

        if self.par_include_filename:
            columns += [FILENAME_COLUMN]

        temp_table = "temp_combined_data"
        self._create_temp_table(temp_table, schema, columns)

        try:
            for path, filename in zip(self.var_pq_files_paths, self.var_pq_files_names):
                logging.info(f"Converting file {path} to csv.")
                self._processFastBatch(path, temp_table, columns, filename)

            total_rows = self.duck.execute(
                f"SELECT COUNT(*) FROM {temp_table}"
            ).fetchone()[0]
            logging.info(f"Exporting total of {total_rows} rows to CSV")

            self.duck.execute(f"""
                COPY {temp_table} TO '{table_path}'
                (HEADER FALSE, DELIMITER ',', QUOTE '"')
            """)

        finally:
            self.duck.execute(f"DROP TABLE IF EXISTS {temp_table}")

        return schema

    def _fillProcess(self, table_path):
        schema = {}

        # First collect all schemas
        for path in self.var_pq_files_paths:
            # Create a temporary view to get schema using DESCRIBE
            view_name = f"temp_view_{abs(hash(path))}"
            self.duck.execute(f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * FROM parquet_scan('{path}')
            """)

            # Get schema using DESCRIBE
            schema_info = self.duck.execute(f"DESCRIBE {view_name}").fetchall()
            file_schema = {col[0]: col[1] for col in schema_info}
            schema.update(file_schema)

            # Clean up the temporary view
            self.duck.execute(f"DROP VIEW IF EXISTS {view_name}")

        if self.par_include_filename:
            schema[FILENAME_COLUMN] = "VARCHAR"

        columns = list(schema.keys())

        os.makedirs(os.path.dirname(table_path), exist_ok=True)

        temp_table = "temp_combined_data"
        self._create_temp_table(temp_table, schema, columns)

        try:
            for path, filename in zip(self.var_pq_files_paths, self.var_pq_files_names):
                view_name = f"temp_view_{abs(hash(path))}"

                try:
                    # Create a temporary view to get schema using DESCRIBE
                    self.duck.execute(f"""
                        CREATE OR REPLACE VIEW {view_name} AS
                        SELECT * FROM parquet_scan('{path}')
                    """)

                    # Get the current file's schema
                    file_schema_info = self.duck.execute(
                        f"DESCRIBE {view_name}"
                    ).fetchall()
                    file_columns = {col[0] for col in file_schema_info}

                    # Create select expressions for each column
                    select_exprs = []
                    for col in columns:
                        if col == FILENAME_COLUMN:
                            select_exprs.append(
                                f"'{filename.replace('\\', '/')}' AS {FILENAME_COLUMN}"
                            )
                            continue

                        if col in file_columns:
                            select_exprs.append(
                                self._get_coalesce_expr(col, schema[col], filename)
                            )
                        else:
                            # Add default value based on type
                            col_type = str(schema[col]).upper()
                            if col_type in ("INTEGER", "BIGINT", "SMALLINT"):
                                select_exprs.append("0 AS " + col)
                            elif col_type in (
                                "DOUBLE",
                                "FLOAT",
                                "REAL",
                                "NUMBER",
                                "NUMERIC",
                            ):
                                select_exprs.append("0.0 AS " + col)
                            elif col_type == "BOOLEAN":
                                select_exprs.append("'False' AS " + col)
                            elif col_type in ("DATE", "TIMESTAMP", "DATETIME"):
                                select_exprs.append("NULL AS " + col)
                            else:
                                select_exprs.append("'' AS " + col)

                    # Insert data using the constructed SELECT
                    self.duck.execute(f"""
                        INSERT INTO {temp_table}
                        SELECT {", ".join(select_exprs)}
                        FROM {view_name}
                    """)

                    row_count = self.duck.execute(
                        f"SELECT COUNT(*) FROM {view_name}"
                    ).fetchone()[0]
                    logging.info(f"Added {row_count} rows from {filename}")

                finally:
                    self.duck.execute(f"DROP VIEW IF EXISTS {view_name}")

            total_rows = self.duck.execute(
                f"SELECT COUNT(*) FROM {temp_table}"
            ).fetchone()[0]
            logging.info(f"Exporting total of {total_rows} rows to CSV")

            self.duck.execute(f"""
                COPY {temp_table} TO '{table_path}'
                (HEADER FALSE, DELIMITER ',', QUOTE '"')
            """)

        finally:
            self.duck.execute(f"DROP TABLE IF EXISTS {temp_table}")

        return schema

    def _strictProcess(self, table_path):
        if self.par_table_columns is None:
            raise UserException(
                'Parameter "columns" must be specified for strict mode.'
            )

        columns = self.par_table_columns
        if self.par_include_filename:
            columns += [FILENAME_COLUMN]

        os.makedirs(os.path.dirname(table_path), exist_ok=True)

        first_path = self.var_pq_files_paths[0]
        # Create a temporary view to get schema using DESCRIBE
        view_name = f"temp_view_{abs(hash(first_path))}"
        self.duck.execute(f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM parquet_scan('{first_path}')
        """)

        # Get schema using DESCRIBE
        schema_info = self.duck.execute(f"DESCRIBE {view_name}").fetchall()
        schema = {col[0]: col[1] for col in schema_info}

        # Clean up the temporary view
        self.duck.execute(f"DROP VIEW IF EXISTS {view_name}")

        temp_table = "temp_combined_data"
        self._create_temp_table(temp_table, schema, columns)

        try:
            for path, filename in zip(self.var_pq_files_paths, self.var_pq_files_names):
                view_name = f"temp_view_{abs(hash(path))}"

                try:
                    # Create a temporary view to get schema using DESCRIBE
                    self.duck.execute(f"""
                        CREATE OR REPLACE VIEW {view_name} AS
                        SELECT * FROM parquet_scan('{path}')
                    """)

                    # Get schema using DESCRIBE
                    schema_info = self.duck.execute(f"DESCRIBE {view_name}").fetchall()
                    file_columns = [col[0] for col in schema_info]

                    missing_columns = list(
                        set(columns) - set(file_columns) - set([FILENAME_COLUMN])
                    )
                    if missing_columns:
                        raise UserException(
                            f"Missing columns {missing_columns} in file {filename}, which were defined "
                            'in configuration parameter "columns".\n'
                            f"Available columns are {file_columns}."
                        )

                    select_cols = [
                        self._get_coalesce_expr(col, schema.get(col, ""), filename)
                        for col in columns
                    ]

                    self.duck.execute(f"""
                        INSERT INTO {temp_table}
                        SELECT {", ".join(select_cols)}
                        FROM {view_name}
                    """)

                    row_count = self.duck.execute(
                        f"SELECT COUNT(*) FROM {view_name}"
                    ).fetchone()[0]
                    logging.info(f"Added {row_count} rows from {filename}")

                finally:
                    self.duck.execute(f"DROP VIEW IF EXISTS {view_name}")

            total_rows = self.duck.execute(
                f"SELECT COUNT(*) FROM {temp_table}"
            ).fetchone()[0]
            logging.info(f"Exporting total of {total_rows} rows to CSV")

            self.duck.execute(f"""
                COPY {temp_table} TO '{table_path}'
                (HEADER FALSE, DELIMITER ',', QUOTE '"')
            """)

        finally:
            self.duck.execute(f"DROP TABLE IF EXISTS {temp_table}")

        return {k: v for k, v in schema.items() if k in columns}

    def run(self):
        self.getParquetFiles()
        self.processParquet()


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
