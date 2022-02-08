import unittest
from functools import reduce
from pathlib import Path
from typing import Dict, Set

from src.transform.AccountType import AccountType
from src.transform.BalanceChange import BalanceChangeAgg
from src.transform.Block import Block
from src.transform.NumberWithScale import NumberWithScale
from src.transform.Transaction import Transaction


class TestTransaction(unittest.TestCase):

    _block: Block

    @classmethod
    def setUpClass(cls):
        cls._block = Block.open(Path(f'resources/blocks/110130000/110130000.json.gz'))

    def test_transactions(self):
        self.assertTrue(self._block.has_transactions(), 'Block should have transactions.')
        self.assertEqual(len(self._block.transactions.only_fee), 3439)
        self.assertEqual(
            set(self._block.transactions),
            set(self._block.transactions.more_than_fee) | set(self._block.transactions.only_fee),
            'Set of transactions more than fee and only fee should include all transactions.'
        )
        self.assertEqual(len(self._block.transactions.successful.only_fee), 3185)
        self.assertEqual(
            set(self._block.transactions.successful),
            set(self._block.transactions.successful.more_than_fee) | set(self._block.transactions.successful.only_fee),
            'Set of successful transactions more than fee and only fee should include all transactions.'
        )

    def test_votes(self):
        self.assertEqual(len(self._block.transactions.votes), 2677)
        self.assertEqual(len(self._block.transactions.successful.votes), 2531)

    def test_fees(self):
        self.assertEqual(self._block.transactions.fees, 17420000)
        self.assertEqual(self._block.transactions.successful.fees, 16000000)

    def test_balance_change(self):
        self.assertEqual(
            self._block.transactions.errors.balance_change(BalanceChangeAgg.OUT),
            NumberWithScale.lamports(-1420000)
        )
        self.assertEqual(
            self._block.transactions.successful.balance_change(BalanceChangeAgg.OUT),
            NumberWithScale.lamports(-149885890118570)
        )

    def test_account_types(self):
        accounts_by_type = self._block.transactions.accounts_by_type
        accounts_by_type_count = {t: len(accounts) for t, accounts in accounts_by_type.items()}

        self.assertEqual(accounts_by_type_count, {
            AccountType.SYSVAR: 4,
            AccountType.PROGRAM: 27,
            AccountType.TOKEN: 211,
            AccountType.COIN: 3480
        })