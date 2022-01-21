from __future__ import annotations

import json
import multiprocessing
from argparse import ArgumentParser
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import List, Set, Callable, Iterable

import dask
import dask.bag as bag
from dask.delayed import Delayed
from distributed import LocalCluster, Client

from src.parse.BalanceChange import BalanceChangeAgg
from src.parse.Block import Block
from src.transform.Interactions import Interactions
from src.transform.Transfer import Transfer

ResultsAndErrors = (List[List[any]], List[List[any]])


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
        config = {
            'temporary_directory': temp_dir,
            'distributed.comm.timeouts.connect': '120s',
            'distributed.comm.timeouts.tcp': '120s'
        }

        with dask.config.set(config), \
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
    def json_to_blocks(json_and_path: (str, str)) -> (Block, List[List[any]]):
        errors = []
        block = None

        try:
            block = Block(json.loads(json_and_path[0]), Path(json_and_path[1]))

            # execute some lazily cached properties to be reused downstream and flush out some errors
            for transaction in block.transactions:
                transaction.instructions
        except Exception as e:
            block = None
            errors.append(['json_to_blocks', json_and_path[1], str(e)])

        return block, errors

    @staticmethod
    def blocks_to_transactions(block: Block) -> ResultsAndErrors:
        rows = []
        errors = []

        for transaction in block.transactions:
            try:
                rows.append([
                    block.epoch(),
                    transaction.signature,
                    transaction.fee,
                    transaction.is_successful,
                    len(transaction.instructions),
                    json.dumps(list(map(lambda a: a.key, transaction.instructions.programs))),
                    len(transaction.accounts),
                    json.dumps({
                        account_type.name: [
                            a.key for a in accounts
                        ] for account_type, accounts in transaction.accounts_by_type.items()
                    }),
                    transaction.total_account_balance_change(BalanceChangeAgg.OUT).v,
                    transaction.total_account_balance_change(BalanceChangeAgg.IN).v,
                    json.dumps(len(transaction.mints)),
                    json.dumps(list(transaction.mints)),
                    json.dumps({mint: change.float for mint, change in
                                transaction.total_token_changes(BalanceChangeAgg.OUT).items()}),
                    json.dumps({mint: change.float for mint, change in
                                transaction.total_token_changes(BalanceChangeAgg.IN).items()}),
                    block.hash,
                    str(block.path)
                ])
            except Exception as e:
                errors.append(['blocks_to_transactions', str(block.path), str(e)])

        return rows, errors

    @staticmethod
    def blocks_to_transfers(block: Block) -> ResultsAndErrors:
        """
        For each block, return a tuple of rows of parsed transfers and rows of errors.
        """
        rows = []
        errors = []

        interactions = Interactions([block])
        for interaction in interactions:
            try:
                if isinstance(interaction, Transfer):
                    rows.append([
                        block.epoch(),
                        interaction.source,
                        interaction.destination,
                        interaction.value.v,
                        interaction.value.scale,
                        interaction.transaction_signature,
                        block.hash,
                        str(block.path)
                    ])
            except Exception as e:
                errors.append(['blocks_to_transfers', str(block.path), str(e)])

        return rows, errors

    def source_and_destinations(
        self, destination_dir: str, keep_subdirs: bool = False
    ) -> list[tuple[str | list[str], Path]]:
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
                        build_glob(subdir_path), destination_path.joinpath(subdir_path.name)
                    ))
            else:
                # one tuple multiple globs since bags don't support **/*
                source_and_destinations.append((
                    list(map(
                        build_glob,
                        [subdir_path for subdir_path in self.blocks_path.iterdir()]
                    )),
                    destination_path
                ))
        else:
            # simple, no subdirectories
            source_and_destinations.append((build_glob(self.blocks_path), destination_path))

        return source_and_destinations

    def write(
        self,
        tasks: Set[FileOutputTask],
        destination_dir: str,
        destination_format: FileOutputFormat,
        keep_subdirs: bool = False
    ):
        """
        Extract transfers from all blocks to file. Optionally keep subdirectory file structure.
        """
        for source, destination in self.source_and_destinations(destination_dir, keep_subdirs):
            blocks_with_errors = bag.read_text(source, include_path=True, files_per_partition=4) \
                .map(FileOutput.json_to_blocks)
            # take the blocks and filter out any that are None due to errors
            blocks = blocks_with_errors.map(lambda r: r[0]).filter(lambda b: b is not None)

            task_results = []
            task_errors = [blocks_with_errors.map(lambda r: r[1])]
            for task in tasks:
                results_with_errors = blocks.map(task.transform)
                base_path = f'{str(destination)}_{str(task.name).lower()}'

                task_results.append(destination_format.to_file(
                    results_with_errors.map(lambda r: r[0]).flatten().to_dataframe(meta=task.meta),
                    base_path
                ))
                task_errors.append(results_with_errors.map(lambda r: r[1]))

            # concat errors across multiple stages
            errors = bag.concat(task_errors).flatten() \
                .to_dataframe(meta=[
                    ('source', 'string'),
                    ('error', 'string'),
                    ('path', 'string')
                ])
            errors = destination_format.to_file(errors, f'{destination}_errors')

            # defer compute of both results so dask will know to reuse intermediate results
            dask.compute(*task_results, errors)


class FileOutputFormat(Enum):
    CSV = (
        lambda delayed, path: delayed.to_csv(f'{path}.csv', index=False, single_file=True, compute=False), 0
    )
    PARQUET = (
        lambda delayed, path: delayed.to_parquet(f'{path}', compute=False), 1
    )

    to_file: Callable[[Delayed, str], Delayed]

    def __init__(self, to_file: Callable[[Delayed, str], Delayed], _):
        self.to_file = to_file


class FileOutputTask(Enum):
    TRANSACTIONS = (
        FileOutput.blocks_to_transactions,
        [
            ('time', 'int64'),
            ('signature', 'string'),
            ('fee', 'int64'),
            ('isSuccessful', 'bool'),
            ('numInstructions', 'int8'),
            ('programs', 'str'),
            ('numAccounts', 'int8'),
            ('accountsByType', 'string'),
            ('lamportsOut', 'int64'),
            ('lamportsIn', 'int64'),
            ('numMints', 'int8'),
            ('mints', 'string'),
            ('tokensOut', 'string'),
            ('tokensIn', 'string'),
            ('blockhash', 'string'),
            ('path', 'string')
        ]
    )
    TRANSFERS = (
        FileOutput.blocks_to_transfers,
        [
            ('time', 'int64'),
            ('source', 'string'),
            ('destination', 'string'),
            ('value', 'int64'),
            ('scale', 'int8'),
            ('transaction', 'string'),
            ('blockhash', 'string'),
            ('path', 'string')
        ]
    )

    transform: Callable[[Block], ResultsAndErrors]
    meta: List[(str, str)]

    @staticmethod
    def all() -> Set[FileOutputTask]:
        return set([task for task in FileOutputTask])

    @staticmethod
    def from_names(names: Iterable[str]) -> Set[FileOutputTask]:
        tasks = set()
        for name in names:
            normalized_name = name.upper()
            if normalized_name == 'ALL':
                return FileOutputTask.all()
            else:
                tasks.add(FileOutputTask[normalized_name])

        return tasks

    def __init__(self, transform: Callable[[Block], ResultsAndErrors], meta: List[(str, str)]):
        self.transform = transform
        self.meta = meta


if __name__ == '__main__':
    parser = ArgumentParser(description='Transform and output block information to file.')

    parser.add_argument('--tasks', nargs='+', help='List of tasks to execute or all.', required=True)

    parser.add_argument('--temp_dir', type=str, help='Temp directory for dask when spilling to disk.', required=True)
    parser.add_argument('--blocks_dir', type=str, help='Source directory for the extracted blocks.', required=True)
    parser.add_argument('--destination_dir', type=str, help='Where to write the results.', required=True)
    parser.add_argument('--destination_format', type=str, help='File format of results.', required=True)

    parser.add_argument('--keep_subdirs', help='Produce results for each subdir of source.', action='store_true')

    args = parser.parse_args()

    with FileOutput.with_local_cluster(temp_dir=args.temp_dir, blocks_dir=args.blocks_dir) as output:
        output.write(
            FileOutputTask.from_names(args.tasks),
            args.destination_dir,
            FileOutputFormat[args.destination_format.upper()],
            args.keep_subdirs
        )
