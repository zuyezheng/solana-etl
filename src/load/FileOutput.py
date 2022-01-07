from __future__ import annotations

import json
import multiprocessing
from argparse import ArgumentParser
from contextlib import contextmanager
from enum import Enum, auto
from pathlib import Path
from typing import List

import dask
import dask.bag as bag
from distributed import LocalCluster, Client

from src.parse.Block import Block
from src.transform.Interaction import Transfer
from src.transform.Interactions import Interactions


class FileOutputFormat(Enum):
    CSV = auto()
    PARQUET = auto()


class FileOutput:
    """
    Output block information to file.

    @author zuyezheng
    """

    blocks_path: Path
    _has_subdirs: bool

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
        self.blocks_path = Path(blocks_dir)
        self._client = client

        # if all files in the blocks directories are directories, then go into each
        self._has_subdirs = all(map(lambda p: p.is_dir(), self.blocks_path.iterdir()))

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

    def write_transfers(
        self,
        destination_dir: str,
        destination_format: FileOutputFormat = FileOutputFormat.CSV,
        keep_subdirs: bool = False
    ):
        """
        Extract transfers from all blocks to file. Optionally keep subdirectory file structure.
        """
        def build_glob(path: Path) -> str:
            return f'{str(path)}/*.json.gz'

        # build out the tuples of destination name and block source glob
        to_process = []
        if self._has_subdirs:
            if keep_subdirs:
                for subdir_path in self.blocks_path.iterdir():
                    to_process.append([f'transfers_{subdir_path.name}', build_glob(subdir_path)])
            else:
                # need to build multiple globs since bags don't support **/*
                to_process.append([
                    'transfers',
                    list(map(
                        build_glob,
                        [subdir_path for subdir_path in self.blocks_path.iterdir()]
                    ))
                ])
        else:
            to_process.append(['transfers', build_glob(self.blocks_path)])

        for destination_name, source_glob in to_process:
            to_df = bag.read_text(source_glob, files_per_partition=20) \
                .map(FileOutput.str_to_transfer_rows) \
                .flatten() \
                .to_dataframe(meta=[
                    ('time', 'int64'),
                    ('source', 'string'),
                    ('destination', 'string'),
                    ('value', 'int64'),
                    ('scale', 'int8'),
                    ('transaction', 'string')
                ])

            if destination_format == FileOutputFormat.PARQUET:
                to_df.to_parquet(f'{destination_dir}/{destination_name}')
            else:
                to_df.to_csv(f'{destination_dir}/{destination_name}.csv', index=False, single_file=True)


if __name__ == '__main__':
    parser = ArgumentParser(description='Transform and output block information to file.')
