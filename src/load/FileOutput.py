from __future__ import annotations

import json
import multiprocessing
from argparse import ArgumentParser
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import List, Set, Callable, Dict, Tuple

import dask
import dask.bag as bag
from distributed import LocalCluster, Client

from src.load.StorageFormat import StorageFormat
from src.load.TransformTask import TransformTask
from src.transform.Block import Block

ResultsAndErrors = Tuple[List[List[any]], List[List[any]]]
Transform = Callable[[Block], ResultsAndErrors]


class FileOutput:
    """
    Output block information to file.

    @author zuyezheng
    """

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

    @staticmethod
    def transform(
        tasks: Dict[str, Transform], json_and_path: (str, str)
    ) -> (Dict[str, List[List[any]]], List[List[any]]):
        """
        Perform all the transform tasks on a given block json and aggregate int a tuple of results in a dictionary by
        task and errors.
        """
        block_source = Path(json_and_path[1]).name

        # default to empty rows for each task for a easy flatten of results vs dealing with Nones
        results = {task_name: [] for task_name in tasks}
        errors = []

        try:
            block = Block(json.loads(json_and_path[0]), block_source)

            # aggregate results and errors for each task
            for task_name in tasks:
                results_and_errors = tasks[task_name](block)
                results[task_name] = results_and_errors[0]
                errors.extend(results_and_errors[1])
        except Exception as e:
            errors.append(['json_to_blocks', block_source, str(e)])

        return results, errors

    blocks_path: Path
    _has_subdirs: bool
    _client: Client

    def __init__(self, blocks_dir: str, client: Client):
        """ Initialize with directory of block extracts. """
        self.blocks_path = Path(blocks_dir)
        self._client = client

        # if all files in the blocks directories are directories, then go into each
        self._has_subdirs = all(map(lambda p: p.is_dir(), self.blocks_path.iterdir()))

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
        tasks: Set[TransformTask],
        destination_dir: str,
        destination_format: StorageFormat,
        keep_subdirs: bool = False
    ):
        """
        Extract transfers from all blocks to file. Optionally keep subdirectory file structure.
        """
        for source, destination in self.source_and_destinations(destination_dir, keep_subdirs):
            # pickling gets tricky with the enum so convert it to a dict with name -> transform
            transforms = {task.name: task.transform for task in tasks}

            results_with_errors = bag.read_text(source, include_path=True, files_per_partition=16) \
                .map(lambda json_and_path: FileOutput.transform(transforms, json_and_path))

            # extract out the specific transform results, flatten, and create a delayed task to output to file
            task_results = []
            for task in tasks:
                task_results.append(destination_format.to_file(
                    results_with_errors
                        .map(partial(lambda task_name, result: result[0][task_name], task.name))
                        .flatten()
                        .to_dataframe(meta=task.meta),
                    f'{str(destination)}_{str(task.name).lower()}'
                ))

            # collect all the errors
            errors = destination_format.to_file(
                results_with_errors.map(lambda r: r[1])
                    .flatten()
                    .to_dataframe(meta=[
                        ('source', 'string'),
                        ('error', 'string'),
                        ('path', 'string')
                    ]),
                f'{destination}_errors'
            )

            # defer compute of both results so dask will know to reuse intermediate results
            dask.compute(*task_results, errors)

def main():
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
            TransformTask.from_names(args.tasks),
            args.destination_dir,
            StorageFormat[args.destination_format.upper()],
            args.keep_subdirs
        )


if __name__ == '__main__':
    main()
