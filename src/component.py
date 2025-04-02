import glob
import json
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
        super().__init__(
            data_path_override=r"C:\Users\alber\DATA\_work\processor-parquet2csv\data"
        )
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

        pass
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

        if (
            dtype == "NUMBER"
            or dtype == "INTEGER"
            or dtype == "BIGINT"
            or dtype == "SMALLINT"
        ):
            return BaseType.integer()
        elif (
            dtype == "DOUBLE"
            or dtype == "FLOAT"
            or dtype == "REAL"
            or dtype == "NUMERIC"
        ):
            return BaseType.float()
        elif dtype == "DATE":
            return BaseType.date()
        elif dtype == "BOOL" or dtype == "BOOLEAN":
            return BaseType.boolean()
        elif dtype == "DATETIME" or dtype == "TIMESTAMP":
            return BaseType.timestamp()
        elif dtype == "DECIMAL":
            return BaseType.numeric()
        else:
            return BaseType.string()

    def createManifest(self, table_path, columns):
        with open(table_path + ".manifest", "w") as _man_file:
            json.dump(
                {
                    "columns": columns,
                    "incremental": self.par_incremental,
                    "primary_key": self.par_primary_keys,
                },
                _man_file,
            )

    def _get_coalesce_expr(self, col, col_type, filename):
        """Helper method to generate COALESCE expression based on column type"""
        if col == FILENAME_COLUMN:
            # Replace backslashes with forward slashes in filename
            safe_filename = filename.replace("\\", "/")
            return f"'{safe_filename}' AS {FILENAME_COLUMN}"

        logging.debug(f"Column {col} has type: {col_type}")
        col_type = str(col_type).upper()

        if col_type == "INTEGER" or col_type == "BIGINT" or col_type == "SMALLINT":
            return f"COALESCE(CAST({col} AS INTEGER), 0) AS {col}"
        elif col_type == "NUMBER" or col_type == "NUMERIC":
            return f"COALESCE({col}, 0) AS {col}"
        elif col_type == "DOUBLE" or col_type == "FLOAT" or col_type == "REAL":
            return f"COALESCE({col}, 0) AS {col}"
        elif col_type == "BOOLEAN":
            return f"COALESCE({col}, false) AS {col}"
        elif col_type == "DATE":
            return f"COALESCE({col}, NULL) AS {col}"
        elif col_type == "TIMESTAMP":
            return f"COALESCE({col}, NULL) AS {col}"
        else:
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
        schema_info = self.duck.execute(
            f"SELECT * FROM parquet_scan('{first_path}') LIMIT 0"
        ).description
        schema = {col[0]: col[1] for col in schema_info}

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
            schema_info = self.duck.execute(
                f"SELECT * FROM parquet_scan('{path}') LIMIT 0"
            ).description
            file_schema = {col[0]: col[1] for col in schema_info}
            schema.update(file_schema)

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
                    # Get the current file's schema
                    file_schema_info = self.duck.execute(
                        f"SELECT * FROM parquet_scan('{path}') LIMIT 0"
                    ).description
                    file_columns = {col[0] for col in file_schema_info}

                    # Create base view
                    self.duck.execute(f"""
                        CREATE OR REPLACE VIEW {view_name} AS
                        SELECT * FROM parquet_scan('{path}')
                    """)

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
        schema_info = self.duck.execute(
            f"SELECT * FROM parquet_scan('{first_path}') LIMIT 0"
        ).description
        schema = {col[0]: col[1] for col in schema_info}

        temp_table = "temp_combined_data"
        self._create_temp_table(temp_table, schema, columns)

        try:
            for path, filename in zip(self.var_pq_files_paths, self.var_pq_files_names):
                view_name = f"temp_view_{abs(hash(path))}"

                try:
                    schema_info = self.duck.execute(
                        f"SELECT * FROM parquet_scan('{path}') LIMIT 0"
                    ).description
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

                    self.duck.execute(f"""
                        CREATE OR REPLACE VIEW {view_name} AS
                        SELECT * FROM parquet_scan('{path}')
                    """)

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
