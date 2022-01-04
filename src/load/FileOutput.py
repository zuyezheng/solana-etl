from __future__ import annotations

import multiprocessing
from contextlib import contextmanager

import dask
import dask.bag as bag
from distributed import LocalCluster, Client


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
