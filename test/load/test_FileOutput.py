import shutil
from pathlib import Path
from unittest import TestCase

import pandas

from src.load.FileOutput import FileOutput, FileOutputFormat


class TestFileOutput(TestCase):

    _test_output_path: Path

    @classmethod
    def setUpClass(cls):
        cls._test_output_path = Path('resources', 'output', cls.__name__)
        cls._test_output_path.mkdir(parents=True)

    @classmethod
    def tearDownClass(cls):
        #shutil.rmtree(cls._test_output_path)
        pass

    def test_transfers(self):
        output_path = self._test_output_path.joinpath('transfers')
        with FileOutput.with_local_cluster(temp_dir='.', blocks_dir='resources/blocks') as output:
            output.write_transfers(
                output_path,
                destination_format=FileOutputFormat.CSV,
                keep_subdirs=True
            )

        # make sure the outputed files contain the right number of transfers
        expected = [
            [110130000, 395],
            [110360000, 196]
        ]
        for block_section, num_transfers in expected:
            df = pandas.read_csv(output_path.joinpath(f'{str(block_section)}_transfers.csv'))
            self.assertEqual((num_transfers, 8), df.shape)
            errors = pandas.read_csv(output_path.joinpath(f'{str(block_section)}_errors.csv'))
            self.assertEqual((0, 3), errors.shape)
