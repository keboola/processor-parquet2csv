# Parquet Processor

## Confifuration:

A sample configuration of the processor:

```json
{
    "definition": {
        "component": "kds-team.processor-parquet2csv"
    },
    "parameters": {
        "mode": "fast",
        "table_name": "test.csv",
        "incremental": true,
        "primary_keys": [
            "order_id"
        ],
        "include_filename": true
    }
}
```

### Parameters

- `mode` - For now, only `fast` option is supported. Further options will be added in the future. This parameters is required.
- `table_name` - A name of the table in storage.
- `incremental` - A boolean value marking, whether to utilize incremental storage.
- `primary_keys` - An array of primary keys.
- `include_filename` - A boolean value. If `true`, an extra column with name of data parquet origin file will be included in the output table.