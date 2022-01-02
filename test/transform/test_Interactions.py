import unittest
from pathlib import Path

from src.parse.Block import Block
from src.transform.Interactions import Interactions


class TestInteractions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._block = Block.open(Path(f'resources/110130000.json.gz'))

    def test_transfers(self):
        transfers = Interactions([self._block])
        self.assertEqual(322, len(transfers))
