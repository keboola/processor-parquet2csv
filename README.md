# Parquet Processor

Parquet processor utilizes DuckDB to process Parquet files and convert them into a single CSV file. A single configuration, in which the Parquet processor is used, should contain the files which belong to one table only and thus maintaining one configuration, one table rule.

Parquet processor automatically filters all files from `/data/in/files` folder, converts them to a CSV format, and outputs to `/data/out/tables` folder.

## Configuration:

A sample of the configuration object:

```json
{
    "definition": {
        "component": "kds-team.processor-parquet2csv"
    },
    "parameters": {
        "mode": "fast",
        "table_name": "table.csv",
        "incremental": true,
        "primary_keys": [
            "order_id"
        ],
        "include_filename": false,
        "debug": true,
        "chunk_size": 5000,
        "columns": [
            "id",
            "order_id",
            "dwh_created",
            "created_at"
        ]
    }
}
```

## Parameters

- `mode` (required) - A mode, which will be utilized in running of the processor. Must be one of `fast`, `fill` or `strict`. See below for further clarification.
- `table_name` (required) - A name of the output table in storage.
- `incremental` (optional) - A boolean value marking, whether to utilize incremental load to storage. If not specified, full load is performed.
- `primary_keys` (optional) - An array of primary keys.
- `include_filename` (optional) - A boolean value. If `true`, an extra column `parquet_filename` with name of data parquet origin file will be included in the output table. Default is `false`.
- `chunk_size` (optional) - A positive integer specifying the size of a chunk, which should be processed in memory. If `chunk_size` is not specified or is a negative number the batch size defaults to 10000.
- `columns` (optional) - An array of columns, which will be read from the Parquet files. If any of the columns specified here is not present in a Parquet file, it will be ignored. If mode is set to `strict`, this parameter is required, since it defines the schema, which should be checked.
- `file_mask` (optional) - A glob-like syntax defining files, which should be included. Can be used for filtering files, or extensions. When not specified, defaults to `*.parquet`, i.e. all files with `.parquet` extension are included.
- `debug` - A boolean value. If `true`, extra logging is added. Default is `false`.

### Different `mode` parameter specification

Since the processor will be processing files continuously, some change in schema of Parquet files may occur with different exports (e.g. new column added or removed). The processor offers different modes to treat the issue that may arise.

#### Mode `fast`

The `fast` mode determines the schema from the first Parquet file it reads and applies this schema to all remaining Parquet files which are processed. This process is fast, but may lead to some data being lost if schema of processed files is different.
If parameter `columns` is specified, the processor will read only specified columns.

#### Mode `fill`

The `fill` mode reads the schemas of all files in the processing queue and creates a unified schema which is applied to all files. If some columns are missing in one of the files, those will be filled with appropriate default values based on the column type:
- Numeric columns (INTEGER, FLOAT): 0
- Boolean columns: false
- Date/Timestamp columns: NULL
- Other types (including STRING): empty string

#### Mode `strict`

Strict mode ensures that all files adhere to the same schema, which is defined by `columns` parameter. If any of the columns defined in `columns` parameter is missing in one of the files, an error is raised and conversion is halted.

## Development

To build and run a docker image, use following commands:

```
docker-compose build dev
docker-compose run --rm dev
```
