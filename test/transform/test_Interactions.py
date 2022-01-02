import unittest
from pathlib import Path

from parse.Block import Block
from transform.Interactions import Interactions


class TestInteractions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._block = Block.open(Path(f'test/resources/110130000.json.gz'))

    def test_transfers(self):
        transfers = Interactions([self._block])
        self.assertEqual(322, len(transfers))
