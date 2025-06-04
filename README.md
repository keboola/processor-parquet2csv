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
        "columns": [
            "id",
            "order_id",
            "dwh_created",
            "created_at"
        ],
        "file_mask": "*.parquet",
        "fill_empty_values": false
    }
}
```

## Parameters

- `mode` (optional) - A mode parameter maintained for backward compatibility with previous pandas implementation. Defaults to "fast". The actual behavior is controlled by `fill_empty_values` parameter.
- `table_name` (required) - A name of the output table in storage.
- `incremental` (optional) - A boolean value marking, whether to utilize incremental load to storage. If not specified, full load is performed.
- `primary_keys` (required) - An array of primary keys.
- `include_filename` (optional) - A boolean value. If `true`, an extra column `parquet_filename` with name of data parquet origin file will be included in the output table. Default is `false`.
- `columns` (optional) - An array of columns, which will be read from the Parquet files. If any of the columns specified here is not present in a Parquet file, it will be ignored.
- `file_mask` (optional) - A glob-like syntax defining files, which should be included. Can be used for filtering files, or extensions. When not specified, defaults to `*.parquet`, i.e. all files with `.parquet` extension are included.
- `debug` (optional) - A boolean value. If `true`, extra logging is added. Default is `false`.
- `fill_empty_values` (optional) - A boolean value. If `true`, missing columns will be filled with appropriate default values based on the column type. Default is `false`.

### Column Type Default Values

When `fill_empty_values` is set to `true`, missing columns will be filled with the following default values:
- Numeric columns (INTEGER, FLOAT): 0
- Boolean columns: false
- Date/Timestamp columns: NULL
- Other types (including STRING): empty string

## Development

To build and run a docker image, use following commands:

```
docker-compose build dev
docker-compose run --rm dev
```
