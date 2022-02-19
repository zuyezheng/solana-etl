from enum import Enum
from typing import Callable

from dask import dataframe
from dask.dataframe import DataFrame
from dask.delayed import Delayed


class StorageFormat(Enum):
    CSV = (
        lambda delayed, path: delayed.to_csv(
            f'{path}.csv',
            index=False,
            single_file=True,
            compute=False
        ),
        lambda path: dataframe.read_csv(path)
    )
    PARQUET = (
        lambda delayed, path: delayed.to_parquet(
            f'{path}',
            compute=False
        ),
        lambda path: dataframe.read_parquet(path)
    )

    to_file: Callable[[Delayed, str], Delayed]
    read_file: Callable[[str], DataFrame]

    def __init__(self, to_file: Callable[[Delayed, str], Delayed], read_file: Callable[[str], DataFrame]):
        self.to_file = to_file
        self.read_file = read_file
