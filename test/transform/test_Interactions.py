import unittest
from pathlib import Path

from src.transform.Block import Block
from src.transform.Interactions import Interactions
from src.transform.Transfer import CoinTransfer, TokenTransfer


class TestInteractions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._block = Block.open(Path(f'resources/blocks/110130000/110130000.json.gz'))

    def test_transfers(self):
        transfers_by_type = Interactions([self._block]).by_type()

        # make sure we get the right number of transfers for each
        self.assertEqual(321, len(transfers_by_type[CoinTransfer]))
        self.assertEqual(73, len(transfers_by_type[TokenTransfer]))
