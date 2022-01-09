from unittest import TestCase

from src.load.FileOutput import FileOutput, FileOutputFormat


class TestFileOutput(TestCase):

    def test_with_local_cluster(self):
        with FileOutput.with_local_cluster(
            '/mnt/nvme_raid/scratch/dask',
            blocks_dir='/mnt/storage/datasets/sol/raw'
        ) as output:
            output.write_transfers(
                '/mnt/scratch_raid/solana/transformed',
                destination_format=FileOutputFormat.CSV,
                keep_subdirs=True
            )

        """
        with FileOutput.with_local_cluster(
            '/mnt/nvme_raid/scratch/dask',
            blocks_dir='resources/blocks'
        ) as output:
            output.write_transfers(
                '/mnt/scratch_raid/solana/transformed/test',
                destination_format=FileOutputFormat.CSV,
                keep_subdirs=True
            )
        """

        self.fail()
