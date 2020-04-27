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
            "dwh_created",
            "created_at"
        ]
    }
}
```

### Parameters

- `mode` - For now, only `fast` option is supported. Further options will be added in the future. This parameters is required.
- `table_name` - A name of the table in storage.
- `incremental` - A boolean value marking, whether to utilize incremental storage.
- `primary_keys` - An array of primary keys.
- `include_filename` - A boolean value. If `true`, an extra column with name of data parquet origin file will be included in the output table.
- `chunk_size` - A positive integer specifying the size of a chunk, which should be processed in memory. In general, chunk size means lower memory consumption, but slower processing. If `chunk_size` is not specified or a negative number, the whole file is processed in memory without batching.