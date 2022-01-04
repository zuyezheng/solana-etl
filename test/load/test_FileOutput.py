from unittest import TestCase

from src.load.FileOutput import FileOutput


class TestFileOutput(TestCase):

    def test_with_local_cluster(self):
        with FileOutput.with_local_cluster(
            '/mnt/nvme_raid/scratch/dask',
            blocks_dir='/mnt/storage/datasets/sol/raw/110280000'
        ) as output:
            output.write_transfers('/mnt/scratch_raid/solana/transformed')

        self.fail()
