import fastparquet
import glob
import json
import logging
import os
import sys
from kbc.env_handler import KBCEnvHandler

KEY_MODE = 'mode'
KEY_TABLE_COLUMNS = 'columns'
KEY_TABLE_NAME = 'table_name'
KEY_INCREMENTAL = 'incremental'
KEY_PRIMARY_KEYS = 'primary_keys'
KEY_FILENAME = 'include_filename'

MANDATORY_PARAMETERS = [KEY_MODE, KEY_TABLE_NAME]
SUPPORTED_MODES = ['fast', 'check', 'strict']

FILENAME_COLUMN = 'parquet_filename'


class ParquetParser(KBCEnvHandler):

    def __init__(self):

        super().__init__(mandatory_params=MANDATORY_PARAMETERS,
                         log_level='INFO')

        self.validateParameters()

    def validateParameters(self):

        try:
            self.validate_config(MANDATORY_PARAMETERS)

        except ValueError as e:
            logging.exception(e)
            sys.exit(1)

        _mode = self.cfg_params[KEY_MODE]

        if _mode not in SUPPORTED_MODES:
            logging.error(f"Unsupported mode {_mode}. Supported modes are: {SUPPORTED_MODES}.")
            sys.exit(1)

        else:
            self.__mode = _mode

        _name = self.cfg_params[KEY_TABLE_NAME]

        if _name.strip() == '':
            logging.error("No table name provided.")
            sys.exit(1)

        else:
            self.__table = _name

        _columns = self.cfg_params.get(KEY_TABLE_COLUMNS, None)

        if not isinstance(_columns, list) and _columns is not None:
            logging.error("Columns to parse must be provided as an array.")
            sys.exit(1)

        elif _columns == []:
            self.__columns = None

        else:
            self.__columns = _columns

        self.__incremental = bool(self.cfg_params.get(KEY_INCREMENTAL, True))
        _pk = self.cfg_params.get(KEY_PRIMARY_KEYS, [])

        if not isinstance(_pk, list):
            logging.error("Primary keys must be provided as an array.")
            sys.exit(1)

        else:
            self.__primary_keys = _pk

        self.__filename = bool(self.cfg_params.get(KEY_FILENAME, False))

    def getParquetFiles(self):

        files_in_path = os.path.join(self.data_path, 'in', 'files')
        all_parquet_files = glob.glob(os.path.join(files_in_path, '**', '*.pq'), recursive=True) + \
            glob.glob(os.path.join(files_in_path, '**', '*.parquet'), recursive=True)

        if len(all_parquet_files) == 0:
            logging.info("No parquet files found.")
            sys.exit(0)

        else:
            logging.debug(all_parquet_files)
            self.files = all_parquet_files

    def processParquet(self):

        path_table = os.path.join(self.tables_out_path, self.__table)
        path_manifest = path_table + '.manifest'

        self._fastProcess(path_table, path_manifest)

        logging.info(f"Converted {len(self.files)} Parquet files to csv.")

    def _fastProcess(self, table_path, manifest_path):

        pq = fastparquet.ParquetFile(self.files)
        parquet_columns = pq.columns

        all_cols = parquet_columns if self.__columns is None else self.__columns
        if self.__filename is True:
            all_cols += [FILENAME_COLUMN]

        try:
            for df, filename in zip(pq.iter_row_groups(columns=self.__columns), self.files):
                if self.__filename is True:
                    df[FILENAME_COLUMN] = filename
                df[all_cols].to_csv(table_path, mode='a', header=False, index=False)

        except ValueError as e:
            logging.exception(f"Exception encountered when converting parquets to a csv. {e}")
            sys.exit(1)

        with open(manifest_path, 'w') as _man:
            json.dump({
                'columns': all_cols,
                'primary_key': self.__primary_keys,
                'incremental': self.__incremental
            }, _man)

    def run(self):
        self.getParquetFiles()
        self.processParquet()


if __name__ == '__main__':
    p = ParquetParser()
    p.run()
