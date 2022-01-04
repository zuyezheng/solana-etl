from __future__ import annotations

import json
import multiprocessing
from argparse import ArgumentParser
from contextlib import contextmanager
from enum import Enum, auto
from typing import List

import dask
import dask.bag as bag
from distributed import LocalCluster, Client

from src.parse.Block import Block
from src.transform.Interaction import Transfer
from src.transform.Interactions import Interactions


class FileOutput:
    """
    Output block information to file.

    @author zuyezheng
    """

    blocks_dir: str

    _client: Client

    @staticmethod
    @contextmanager
    def with_local_cluster(
        temp_dir: str,
        n_workers: int = multiprocessing.cpu_count(),
        **kwargs
    ) -> FileOutput:
        """
        Create with a new dask client with a local cluster. kwargs should include for FileOutput except for client.
        """
        with dask.config.set({'temporary_directory': temp_dir}), \
            LocalCluster(n_workers=n_workers, threads_per_worker=1) as cluster, \
            Client(cluster, timeout=120) as client:
            kwargs['client'] = client
            yield FileOutput(**kwargs)

    def __init__(self, blocks_dir: str, client: Client):
        """ Initialize with directory of block extracts. """
        self.blocks_dir = blocks_dir
        self._client = client

    @staticmethod
    def str_to_transfer_rows(json_str: str) -> List[List[any]]:
        block = Block(json.loads(json_str))
        interactions = Interactions([block])

        rows = []
        for interaction in interactions:
            if isinstance(interaction, Transfer):
                rows.append([
                    block.epoch(),
                    interaction.source,
                    interaction.destination,
                    interaction.value.v,
                    interaction.value.scale,
                    interaction.transaction_signature
                ])

        return rows

    def write_transfers(self, destination: str, keep_subdir: bool = False):
        """
        Extract transfers from all blocks to file. Optionally keep subdirectory file structure.
        """
        bag.read_text(f'{self.blocks_dir}/*.json.gz') \
            .map(FileOutput.str_to_transfer_rows) \
            .flatten() \
            .to_dataframe(meta=[
                ('time', 'int64'),
                ('source', 'string'),
                ('destination', 'string'),
                ('value', 'int64'),
                ('scale', 'int8'),
                ('transaction', 'string')
            ]) \
            .to_csv(f'{destination}/transfers.csv', index=False, single_file=True)


class FileOutputFormat(Enum):
    CSV: auto()
    PARQUET: auto()


if __name__ == '__main__':
    parser = ArgumentParser(description='Transform and output block information to file.')
