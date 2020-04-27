import csv
import fastparquet
import glob
import json
import logging
import os
import sys
import pyarrow.parquet as pq
from kbc.env_handler import KBCEnvHandler

KEY_MODE = 'mode'
KEY_TABLE_COLUMNS = 'columns'
KEY_TABLE_NAME = 'table_name'
KEY_INCREMENTAL = 'incremental'
KEY_PRIMARY_KEYS = 'primary_keys'
KEY_FILENAME = 'include_filename'
KEY_CHUNKSIZE = 'chunk_size'
KEY_DEBUG = 'debug'

MANDATORY_PARAMETERS = [KEY_MODE, KEY_TABLE_NAME]
SUPPORTED_MODES = ["fast"]  # , "fill", "script"]

FILENAME_COLUMN = 'parquet_filename'


class ParquetParser(KBCEnvHandler):

    def __init__(self):

        super().__init__(mandatory_params=MANDATORY_PARAMETERS, log_level='INFO')

        try:
            self.validate_config(MANDATORY_PARAMETERS)

        except ValueError as e:
            logging.exception(f"Missing mandatory fields {e} in configuration.")
            sys.exit(1)

        self.par_mode = self.cfg_params[KEY_MODE]
        self.par_table_name = self.cfg_params[KEY_TABLE_NAME]
        self.par_table_columns = self.cfg_params.get(KEY_TABLE_COLUMNS, [])
        self.par_incremental = bool(self.cfg_params.get(KEY_INCREMENTAL, False))
        self.par_primary_keys = self.cfg_params.get(KEY_PRIMARY_KEYS, [])
        self.par_include_filename = bool(self.cfg_params.get(KEY_FILENAME, False))
        self.par_chunk_size = self.cfg_params.get(KEY_CHUNKSIZE, None)
        self.par_debug = self.cfg_params.get(KEY_DEBUG, False)
        self.files_in_path = os.path.join(self.data_path, 'in', 'files')

        if self.par_debug is True:
            logging.getLogger().setLevel('DEBUG')
        else:
            pass

        self.validateParameters()

    def validateParameters(self):

        # mode validation
        if self.par_mode not in SUPPORTED_MODES:
            logging.error(f"Unsupported mode {self.par_mode}. Supported modes are: {SUPPORTED_MODES}.")
            sys.exit(1)

        # table name validation
        if not isinstance(self.par_table_name, str):
            logging.error(f"Parameter \"table_name\" must be of type string.")
            sys.exit(1)

        elif self.par_table_name.strip() == '':
            logging.error("No table name provided.")
            sys.exit(1)

        elif self.par_table_name.endswith('.csv') is False:
            self.par_table_name = self.par_table_name + '.csv'

        else:
            pass

        # table columns validation
        if not isinstance(self.par_table_columns, list):
            logging.error("Parameter \"columns\" must be of type list.")
            sys.exit(1)

        elif self.par_table_columns == []:
            self.par_table_columns = None

        else:
            pass

        if not isinstance(self.par_primary_keys, list):
            logging.error("Parameter \"primary_keys\" must be of type list.")
            sys.exit(1)

        else:
            pass

        if self.par_chunk_size is None:
            pass

        else:
            try:
                _cs = int(self.par_chunk_size)

            except ValueError:
                logging.exception("Parameter \"chunk_size\" must be either an integer or \"null\".")
                sys.exit(1)

            self.par_chunk_size = _cs if _cs > 0 else None
            logging.debug(f"Chunk size set to {self.par_chunk_size}.")

    def getParquetFiles(self):

        all_parquet_files = glob.glob(os.path.join(self.files_in_path, '**', '*.parquet'), recursive=True)

        if len(all_parquet_files) == 0:
            logging.info("No parquet files found.")
            sys.exit(0)

        else:
            self.var_pq_files_paths = all_parquet_files
            self.var_pq_files_names = [x.replace(self.files_in_path, '') for x in all_parquet_files]

    def processParquet(self):

        path_table = os.path.join(self.tables_out_path, self.par_table_name)

        if self.par_mode == 'fast':
            _columns = self._fastProcess(path_table)
        self.createManifest(path_table, columns=_columns)

        logging.info(f"Converted {len(self.var_pq_files_names)} Parquet files to csv.")

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

    def _fastProcess(self, table_path):

        schema = None
        columns = None

        with open(table_path, 'w') as out_results:

            for path, filename in zip(self.var_pq_files_paths, self.var_pq_files_names):

                _pq_file = pq.read_table(path, columns=self.par_table_columns)

                if schema is None:
                    schema = _pq_file.schema

                    logging.debug(f"Using following schema to parse the files:\n{schema}.")

                    if self.par_table_columns is None:
                        columns = schema.names
                    else:
                        columns = self.par_table_columns

                    if self.par_include_filename is True:
                        columns += [FILENAME_COLUMN]

                else:
                    pass

                _pq_batches = _pq_file.to_batches(max_chunksize=self.par_chunk_size)

                for _pq_batch in _pq_batches:

                    _df_batch = _pq_batch.to_pandas()

                    for _c in columns:

                        if _c not in _df_batch.columns:
                            if _c == FILENAME_COLUMN and self.par_include_filename is True:
                                _df_batch[_c] = filename

                            elif _c == FILENAME_COLUMN and self.par_include_filename is False:
                                continue

                            else:
                                _df_batch[_c] = ''

                    _df_batch[columns].to_csv(out_results, header=False, index=False, na_rep='')

                logging.debug(f"Converted {filename} to csv. Rows: {_pq_file.num_rows}.")

        return columns

    def _fastProcess_pyarrow(self, table_path):

        _wrt = None

        with open(table_path, 'w') as out_table:

            for f, filename in zip(self.files, self.files_clean):
                _pq = pq.read_table(f)
                pq_batches = _pq.to_batches(max_chunksize=5000)
                parquet_columns = _pq.schema.names
                all_cols = parquet_columns if self.__columns is None else self.__columns
                if self.__filename is True:
                    all_cols += [FILENAME_COLUMN]

                if _wrt is None:
                    _wrt = csv.DictWriter(out_table, fieldnames=all_cols,
                                          restval='', extrasaction='ignore',
                                          quotechar='\"', quoting=csv.QUOTE_ALL)
                    _wrt.writeheader()

                for _b in pq_batches:
                    pdb = _b.to_pandas().fillna('')

                    for _, row in pdb.iterrows():
                        _wrt.writerow(row)

    def _fastProcess_deprecated(self, table_path, manifest_path):

        _wrt = None

        with open(table_path, 'w') as out_table:

            for f, filename in zip(self.files, self.files_clean):
                pq = fastparquet.ParquetFile(f)
                parquet_columns = pq.columns

                all_cols = parquet_columns if self.__columns is None else self.__columns
                if self.__filename is True:
                    all_cols += [FILENAME_COLUMN]

                if _wrt is None:
                    _wrt = csv.DictWriter(out_table, fieldnames=all_cols,
                                          restval='', extrasaction='ignore',
                                          quotechar='\"', quoting=csv.QUOTE_ALL)

                try:
                    df = pq.to_pandas().fillna('')
                    if self.__filename is True:
                        df[FILENAME_COLUMN] = filename
                    # df[all_cols].to_csv(table_path, mode='a', header=False, index=False)

                    for _, row in df.iterrows():
                        _wrt.writerow(row)

                except ValueError as e:
                    logging.exception(f"Exception encountered when converting parquets to a csv. {e}")
                    sys.exit(1)

    def run(self):
        self.getParquetFiles()
        self.processParquet()


if __name__ == '__main__':
    p = ParquetParser()
    p.run()
