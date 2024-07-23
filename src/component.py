import glob
import json
import logging
import os

import pyarrow
from pyarrow import parquet as pq, types, string
import pandas as pd
from keboola.component import ComponentBase, UserException
from keboola.component.dao import BaseType, ColumnDefinition

KEY_MODE = 'mode'
KEY_TABLE_COLUMNS = 'columns'
KEY_TABLE_NAME = 'table_name'
KEY_INCREMENTAL = 'incremental'
KEY_PRIMARY_KEYS = 'primary_keys'
KEY_FILENAME = 'include_filename'
KEY_CHUNKSIZE = 'chunk_size'
KEY_DEBUG = 'debug'
KEY_EXTENSION_MASK = 'file_mask'

MANDATORY_PARAMETERS = [KEY_MODE, KEY_TABLE_NAME]
SUPPORTED_MODES = ["fast", "fill", "strict"]  # , "pandas"]
FILENAME_COLUMN = 'parquet_filename'

DEFAULT_CHUNK_SIZE = 10000


class Component(ComponentBase):

    def __init__(self):
        ComponentBase.__init__(self)
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
        self.par_extension_mask = self.cfg_params.get(KEY_EXTENSION_MASK, '*.parquet')

        if self.par_debug is True:
            logging.getLogger().setLevel('DEBUG')
        else:
            pass

        self.validateParameters()

    def validateParameters(self):

        # mode validation
        if self.par_mode not in SUPPORTED_MODES:
            raise UserException(f"Unsupported mode {self.par_mode}. Supported modes are: {SUPPORTED_MODES}.")

        # table name validation
        if not isinstance(self.par_table_name, str):
            raise UserException("Parameter \"table_name\" must be of type string.")

        elif self.par_table_name.strip() == '':
            raise UserException("No table name provided.")

        elif self.par_table_name.endswith('.csv') is False:
            self.par_table_name = self.par_table_name + '.csv'

        else:
            pass

        # table columns validation
        if not isinstance(self.par_table_columns, list):
            raise UserException("Parameter \"columns\" must be of type list.")

        elif len(self.par_table_columns) == 0:
            self.par_table_columns = None

        if not isinstance(self.par_primary_keys, list):
            raise UserException("Parameter \"primary_keys\" must be of type list.")

        if self.par_chunk_size is None:
            self.par_chunk_size = DEFAULT_CHUNK_SIZE

        else:
            try:
                _cs = int(self.par_chunk_size)

            except ValueError:
                raise UserException("Parameter \"chunk_size\" must be either an integer or \"null\".")

            self.par_chunk_size = _cs if _cs > 0 else DEFAULT_CHUNK_SIZE

    def getParquetFiles(self):

        all_parquet_files = glob.glob(os.path.join(self.files_in_path, '**', self.par_extension_mask), recursive=True)
        # to ensure consistent order on all platforms
        all_parquet_files.sort()

        if len(all_parquet_files) == 0:
            raise UserException("No parquet files found.")

        else:
            nonempty_parquet_files = [path for path in all_parquet_files if os.path.getsize(path) > 0]
            empty_parquet_files = [path for path in all_parquet_files if os.path.getsize(path) == 0]

            logging.info(f"Skipping {len(empty_parquet_files)} empty files.")
            # logging.debug(f"Paths of empty files: {[x.replace(self.files_in_path, '') for x in empty_parquet_files]}.") # noqa

            self.var_pq_files_paths = nonempty_parquet_files
            self.var_pq_files_names = [x.replace(self.files_in_path, '') for x in nonempty_parquet_files]

            logging.debug(f"Processing {len(self.var_pq_files_names)} files. Names:\n{self.var_pq_files_names}.")

    def processParquet(self):

        path_table = os.path.join(self.tables_out_path, self.par_table_name)

        if self.par_mode == 'fast':
            _schema = self._fastProcess(path_table)

        elif self.par_mode == 'fill':
            _schema = self._fillProcess(path_table)

        elif self.par_mode == 'strict':
            _schema = self._strictProces(path_table)

        # elif self.par_mode == 'pandas':
        #     self._pandasProcess(path_table)

        else:
            raise UserException(f"Unsupported mode {self.par_mode}.")

        schema = {k: ColumnDefinition(data_types=self.convert_dtypes(v)) for k, v in _schema.items()}
        out_table = self.create_out_table_definition(self.par_table_name, schema=schema,
                                                     primary_key=self.par_primary_keys)
        self.write_manifest(out_table)

        logging.info(f"Converted {len(self.var_pq_files_names)} Parquet files to csv.")

    def convert_dtypes(self, dtype: pyarrow.DataType) -> BaseType:
        if types.is_integer(dtype):
            return BaseType.integer()
        elif types.is_floating(dtype):
            return BaseType.float()
        elif types.is_boolean(dtype):
            return BaseType.boolean()
        elif types.is_date(dtype):
            return BaseType.date()
        elif types.is_timestamp(dtype):
            return BaseType.timestamp()
        elif types.is_decimal(dtype):
            return BaseType.numeric()
        else:
            return BaseType.string()

    def createManifest(self, table_path, columns):

        with open(table_path + '.manifest', 'w') as _man_file:

            json.dump(
                {
                    'columns': columns,
                    'incremental': self.par_incremental,
                    'primary_key': self.par_primary_keys
                },
                _man_file
            )

    # def _pandasProcess(self, table_path):

    #     with open(table_path, 'w') as out_results:

    #         for path, filename in zip(self.var_pq_files_paths, self.var_pq_files_names):

    #             logging.info(f"Converting file {path} to csv.")
    #             _df = pd.read_parquet(path, memory_map=False)
    #             _df.to_csv(out_results, index=False, header=False)

    def _processFastBatch(self, pq_path, out_table, columns, filename):

        _pq_file = pq.read_table(pq_path, columns=self.par_table_columns)
        _pq_batches = _pq_file.to_batches(max_chunksize=self.par_chunk_size)

        for _pq_batch in _pq_batches:
            _df_batch = pd.DataFrame(_pq_batch.to_pydict(), dtype=str)

            for _c in columns:

                if _c not in _df_batch.columns:
                    if _c == FILENAME_COLUMN:
                        _df_batch[_c] = filename

                    else:
                        _df_batch[_c] = ''

            _df_batch[columns].to_csv(out_table, header=False, index=False, na_rep='')

        logging.debug(f"Converted {pq_path} to csv. Rows: {_pq_file.num_rows}.")

    def _fastProcess(self, table_path):

        schema = None
        columns = None

        with open(table_path, 'w') as out_results:

            for path, filename in zip(self.var_pq_files_paths, self.var_pq_files_names):
                logging.info(f"Converting file {path} to csv.")

                _pq_file_schema = pq.read_schema(path)

                if schema is None:
                    schema = _pq_file_schema

                    if schema.names == [] and self.par_table_columns is not None:
                        raise UserException("Schema is empty. Make sure parameter \"columns\" specifies" +
                                            "correct columns present in schema.")

                    elif schema.names == []:
                        raise UserException("Schema is empty.")

                    else:
                        pass

                    logging.debug(f"Using following schema to parse the files:\n{schema}.")

                    if self.par_table_columns is None:
                        columns = schema.names
                    else:
                        columns = self.par_table_columns

                    if self.par_include_filename is True:
                        columns += [FILENAME_COLUMN]

                else:
                    pass

                self._processFastBatch(path, out_results, columns, filename)

                # _pq_file = pq.read_table(path, columns=self.par_table_columns)
                # _pq_batches = _pq_file.to_batches(max_chunksize=self.par_chunk_size)

                # for _pq_batch in _pq_batches:
                #     _df_batch = pd.DataFrame(_pq_batch.to_pydict(), dtype=str)

                #     for _c in columns:

                #         if _c not in _df_batch.columns:
                #             if _c == FILENAME_COLUMN:
                #                 _df_batch[_c] = filename

                #             else:
                #                 _df_batch[_c] = ''

                #     _df_batch[columns].to_csv(out_results, header=False, index=False, na_rep='')

                # logging.debug(f"Converted {filename} to csv. Rows: {_pq_file.num_rows}.")

        return dict(zip(schema.names, schema.types))

    def _fillProcess(self, table_path):

        schema = {}

        for path in self.var_pq_files_paths:

            _pq_file = pq.read_table(path, columns=self.par_table_columns)
            schema.update(dict(zip(_pq_file.schema.names, _pq_file.schema.types)))

        logging.debug(schema.keys())

        if self.par_include_filename is True:
            schema.update({FILENAME_COLUMN: string()})

        with open(table_path, 'w') as out_results:

            for path, filename in zip(self.var_pq_files_paths, self.var_pq_files_names):

                _pq_file = pq.read_table(path)

                _pq_batches = _pq_file.to_batches(max_chunksize=self.par_chunk_size)

                for _pq_batch in _pq_batches:

                    _df_batch = pd.DataFrame(_pq_batch.to_pydict(), dtype=str)

                    for _c in schema:

                        if _c not in _df_batch.columns:
                            if _c == FILENAME_COLUMN:
                                _df_batch[_c] = filename

                            else:
                                _df_batch[_c] = ''

                    _df_batch[schema.keys()].to_csv(out_results, header=False, index=False, na_rep='')

                logging.debug(f"Converted {filename} to csv. Rows: {_pq_file.num_rows}.")

        return schema

    def _strictProces(self, table_path):

        if self.par_table_columns is None:
            raise UserException("Parameter \"columns\" must be specified for strict mode.")

        columns = self.par_table_columns
        if self.par_include_filename is True:
            columns += [FILENAME_COLUMN]

        with open(table_path, 'w') as out_results:

            for path, filename in zip(self.var_pq_files_paths, self.var_pq_files_names):

                _pq_file = pq.read_table(path)

                missing_columns = list(set(columns) - set(_pq_file.schema.names) - set([FILENAME_COLUMN]))
                if missing_columns != []:
                    raise UserException(f"Missing columns {missing_columns} in file {filename}, which were defined " +
                                        "in configuration parameter \"columns\".\n" +
                                        f"Available columns are {_pq_file.schema.names}.")

                _pq_batches = _pq_file.to_batches(max_chunksize=self.par_chunk_size)

                for _pq_batch in _pq_batches:

                    _df_batch = pd.DataFrame(_pq_batch.to_pydict(), dtype=str)
                    if FILENAME_COLUMN in columns:
                        _df_batch[FILENAME_COLUMN] = filename

                    _df_batch[columns].to_csv(out_results, header=False, index=False, na_rep='')

                logging.debug(f"Converted {filename} to csv. Rows: {_pq_file.num_rows}.")

                schema = dict(zip(_pq_file.schema.names, _pq_file.schema.types))

                filtered_schema = {k: v for k, v in schema.items() if k in columns}

        return filtered_schema

    def run(self):
        self.getParquetFiles()
        self.processParquet()


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
