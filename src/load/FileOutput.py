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
from dask import delayed
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
    def transfers_with_errors(json_and_path: [str, str]) -> tuple[List[List[any]], List[List[any]]]:
        """
        For each json blob and path, return a tuple of rows of parsed transfers and rows of errors.
        """
        rows = []
        errors = []
        try:
            block = Block(json.loads(json_and_path[0]))
            interactions = Interactions([block])

            for interaction in interactions:
                if isinstance(interaction, Transfer):
                    rows.append([
                        block.epoch(),
                        interaction.source,
                        interaction.destination,
                        interaction.value.v,
                        interaction.value.scale,
                        interaction.transaction_signature,
                        json_and_path[1]
                    ])
        except Exception as e:
            errors.append([json_and_path[1], str(e)])

        return rows, errors

    def source_and_destinations(
        self, destination_dir: str, keep_subdirs: bool = False
    ) -> list[tuple[str | list[str], str]]:
        """
        Build the tuples of source glob and destination path. There could be multiple tuples for subdirectories and
        some sources could be a list of globs.
        """
        destination_path = Path(destination_dir)

        def build_glob(path: Path) -> str:
            return f'{str(path)}/*.json.gz'

        source_and_destinations = []
        if self._has_subdirs:
            if keep_subdirs:
                # build out tuple for each subdirectory
                for subdir_path in self.blocks_path.iterdir():
                    source_and_destinations.append((
                        build_glob(subdir_path), str(destination_path.joinpath(f'transfers_{subdir_path.name}'))
                    ))
            else:
                # one tuple multiple globs since bags don't support **/*
                source_and_destinations.append((
                    list(map(
                        build_glob,
                        [subdir_path for subdir_path in self.blocks_path.iterdir()]
                    )),
                    str(destination_path.joinpath('transfers'))
                ))
        else:
            # simple, no subdirectories
            source_and_destinations.append((
                build_glob(self.blocks_path), str(destination_path.joinpath('transfers'))
            ))

        return source_and_destinations

    def write_transfers(
        self,
        destination_dir: str,
        destination_format: FileOutputFormat = FileOutputFormat.CSV,
        keep_subdirs: bool = False
    ):
        """
        Extract transfers from all blocks to file. Optionally keep subdirectory file structure.
        """
        for source, destination in self.source_and_destinations(destination_dir, keep_subdirs):
            transfers_with_errors = bag.read_text(source, include_path=True, files_per_partition=20) \
                .map(FileOutput.transfers_with_errors)

            errors = transfers_with_errors.map(lambda t_e: t_e[1]) \
                .flatten() \
                .to_dataframe(meta=[
                    ('error', 'string'),
                    ('path', 'string')
                ]) \
                .to_csv(f'{destination}_errors.csv', index=False, single_file=True, compute=False)

            transfers = transfers_with_errors.map(lambda t_e: t_e[0]) \
                .flatten() \
                .to_dataframe(meta=[
                    ('time', 'int64'),
                    ('source', 'string'),
                    ('destination', 'string'),
                    ('value', 'int64'),
                    ('scale', 'int8'),
                    ('transaction', 'string'),
                    ('path', 'string')
                ])
            if destination_format == FileOutputFormat.PARQUET:
                transfers = transfers.to_parquet(destination, compute=False)
            else:
                transfers = transfers.to_csv(f'{destination}.csv', index=False, single_file=True, compute=False)

            # defer compute of both results so dask will know to reuse intermediate results
            dask.compute(transfers, errors)


if __name__ == '__main__':
    parser = ArgumentParser(description='Transform and output block information to file.')
