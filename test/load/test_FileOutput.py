import shutil
from pathlib import Path
from unittest import TestCase

import pandas

from src.load.FileOutput import FileOutput, FileOutputFormat, FileOutputTask


class TestFileOutput(TestCase):

    _test_destination_path: Path

    @classmethod
    def setUpClass(cls):
        cls._test_destination_path = Path('resources', 'output', cls.__name__)
        cls._test_destination_path.mkdir(parents=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls._test_destination_path)

    def test_transfers(self):
        destination_path = self._test_destination_path.joinpath('transfers')
        with FileOutput.with_local_cluster(temp_dir='.', blocks_dir='resources/blocks') as output:
            output.write(
                [FileOutputTask.TRANSACTIONS, FileOutputTask.TRANSFERS],
                destination_path,
                FileOutputFormat.CSV,
                True
            )

        # make sure the outputed files contain the right number of transfers
        expected = [
            [110130000, 394],
            [110360000, 194]
        ]
        for block_section, num_transfers in expected:
            df = pandas.read_csv(destination_path.joinpath(f'{str(block_section)}_transfers.csv'))
            self.assertEqual((num_transfers, 8), df.shape)
            errors = pandas.read_csv(destination_path.joinpath(f'{str(block_section)}_errors.csv'))
            self.assertEqual((0, 3), errors.shape)
