**Extract** blocks from the Solana RPC as JSON, **transform** those into object representations, and **load** them into destinations as denormalized tables (CSVs or parquet) or a graph representation for analysis in your analytics tool of choice.

## Install

```
pip install git+https://github.com/zuyezheng/solana-etl
```

##  Run

Extraction will default to using `https://api.mainnet-beta.solana.com` if `endpoint` is not provided. A `start` and `end` slot can be used to configure which blocks to extract. If end is not provided, extract will continue indefinitely until stopped, pausing and retrying when reaching slots that are not yet available. If `start` is greater than `end`, extract will count down from the higher slot.

To avoid files that get too large or a single directory with too many blocks, `slots_per_file` and `slots_per_dir` can be used to group blocks into something reasonable during extract.

### Tasks

You can specify which specific tasks you want to use from transforms or `all`. Specific schemas for each can be found in [TransformTask](https://github.com/zuyezheng/solana-etl/blob/master/src/load/TransformTask.py).

- **Transactions**: All transactions including those that errored out with things like number of transactions, accounts, mints as well as serialized JSON for coin and token changes.
- **Transfers**: All successful transforms for coins and tokens. `values` are stored unscaled with an adjacent `scale` column.

### Streaming

Stream directly from Solana RPC to transforms and loaded to file. A CSV will be produced for errors as well as each task grouped by `slots_per_file`.

```
solana-extract-streaming output_loc
    --tasks TASKS [TASKS ...] 
    [--endpoint ENDPOINT] 
    [--start START] 
    [--end END]
    [--slots_per_file SLOTS_PER_FILE]
    
solana-extract-streaming /mnt/storage/foo
    --tasks all
    --start 119_000_000
```

### Batch

Extract raw block json to compressed file and then batch process them into forms more useful for analytics. Extracting raw blocks is not cheap unless you have your own API node so useful to have around for future transforms and load use cases.

```
solana-extract-batch output_loc
    [--endpoint ENDPOINT] 
    [--start START] 
    [--end END] 
    [--slots_per_dir SLOTS_PER_DIR]
```

Use dask to batch process into something useful.

```
solana-load-file 
    --tasks TASKS [TASKS ...] 
    --temp_dir TEMP_DIR 
    --blocks_dir BLOCKS_DIR 
    --destination_dir DESTINATION_DIR 
    --destination_format DESTINATION_FORMAT 
    [--keep_subdirs]
```
