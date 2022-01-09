import unittest
from pathlib import Path

from src.parse.BalanceChange import BalanceChangeAgg
from src.parse.Block import Block
from src.parse.Transaction import Transaction


class TestBlock(unittest.TestCase):

    _block: Block
    _interesting_transaction: Transaction
    _transaction_with_tokens: Transaction

    @classmethod
    def setUpClass(cls):
        cls._block = Block.open(Path(f'resources/blocks/110130000/110130000.json.gz'))
        cls._interesting_transaction = cls._block.find_transaction(
            '2XMqtpXpp83pupsM5iiie2s69iRTHrV6oA6zxDTY9hRC4M2Rr9Yh5knSkBZbk22Wt7Qv88akacJifnaX6oL5ncqS'
        )
        cls._transaction_with_tokens = cls._block.find_transaction(
            '44DLZ5ezRVvibgxwc4erA4ywQ7XUyf3DvPyt3uqsQR1ucZs2wSrBcHJRe7V2P2FoYJK9XPXNsp4mAnVX8sLXpvin'
        )

    def test_transactions(self):
        self.assertTrue(self._block.has_transactions(), 'Block should have transactions.')
        self.assertEqual(
            set(self._block.transactions),
            set(self._block.transactions.more_than_fee()) | set(self._block.transactions.only_fee()),
            'Set of transactions more than fee and only fee should include all transactions.'
        )

    def test_balance_changes(self):
        self.assertEqual(
            {
                '11111111111111111111111111111111': 0.0,
                '4QuHa8NuHCFvx2XgYG5F5LyG8CpaHXXaDbn3ouv9khxh': 0.0,
                '5KFsC5mLg6d2MbJgCs4k2qJVampgPoZFeuSwsUjSN2gJ': 0.00203928,
                '6DLUecp4G13R4BCANcYZm3W3A55vm8ith7VscMAr8wV3': -0.0119812,
                '6vV7x9Gzrkd7HGds9JyQ3NuMNao3aVBJtyL1aJLM5CKY': 0.0028536,
                '7WK1nq1iCw6W2Da5PM5dihn8iCEvjBC3QAzEHKYFGfNY': 0.00561672,
                '7zAhomM86b2LCtRniiSUor1qEYSH2LWUBB4WJXCzeEts': 0.0,
                'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL': 0.0,
                'E2HeNtruwL6bcd6XSqKGk5ucw43jrNsThFHoSmTNeSbi': 0.0014616,
                'SysvarC1ock11111111111111111111111111111111': 0.0,
                'SysvarRent111111111111111111111111111111111': 0.0,
                'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA': 0.0,
                'cndyAnrLdpjq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ': 0.0,
                'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s': 0.0
            },
            dict(map(
                lambda c: (c.account.key, c.change.float),
                self._interesting_transaction.account_balance_changes.values()
            ))
        )

        self.assertEqual(
            -0.00001,
            self._interesting_transaction.total_account_balance_change().float,
            'Signed change should be the fee.'
        )

        self.assertEqual(
            0.0239524,
            self._interesting_transaction.total_account_balance_change(BalanceChangeAgg.ABS).float
        )

        self.assertEqual(
            -0.0119812,
            self._interesting_transaction.total_account_balance_change(BalanceChangeAgg.OUT).float
        )

        self.assertEqual(
            0.0119712,
            self._interesting_transaction.total_account_balance_change(BalanceChangeAgg.IN).float
        )

    def test_token_balance_changes(self):
        self.assertEqual(
            {
                '5cR1yJcjMaHLAPMqXEZc6zaTazn1fiDctXG39crTUfq3': 0.284203,
                '6H4TkDcHEWkyM2LVNkHdmBsZym4b7Hf5SYfq4HRMbtHR': 4839.201077,
                '6rQjE7ve9vmZw2L988mRCgWUJAeBGKLJHjq1oeFbf7Fb': 24.317978,
                '7jMC3ZYQtRQycDwSTVKxyXhvpoU5C1T4ENJoYNjWLJ6T': 0.0,
                '8fEdArAuMR3b44WQK1UL1fDLB3kDK2N9whhDRP4sWP5v': -4863.519055,
                '9BnkuYqwYdrp7A8kV8V3uPRb21Kjihc7C8eg6jmMz1dm': 12.5874,
                'AqWzsrvrTumzGNgLadaiu5Bz46xE4q3CZApQRUqsbbu9': 0.012599,
                'DmmSN7NH3FpKSkfNuE2MbbWPoW1uVrFdJDVtrCeuo5Wi': -12.884202,
                'GjKZHWYCikFFMLUozYLzCnR5kJp61LqQkLPEtv5aJB4k': 0.0
            },
            dict(map(
                lambda c: (c.account.key, c.change.float),
                self._transaction_with_tokens.token_balance_changes.values()
            ))
        )

        self.assertEqual(
            {
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 0.0,
                'EWS2ATMt5fQk89NWLJYNRmGaNoji8MhFZkUB4DiWCCcz': 0.0
            },
            dict(map(
                lambda kv: (kv[0], kv[1].float),
                self._transaction_with_tokens.total_token_changes().items()
            )),
            'Tokens shouldn\'t disappear.'
        )

        self.assertEqual(
            {
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 25.768404,
                'EWS2ATMt5fQk89NWLJYNRmGaNoji8MhFZkUB4DiWCCcz': 9727.03811
            },
            dict(map(
                lambda kv: (kv[0], kv[1].float),
                self._transaction_with_tokens.total_token_changes(BalanceChangeAgg.ABS).items()
            ))
        )

        self.assertEqual(
            {
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': -12.884202,
                'EWS2ATMt5fQk89NWLJYNRmGaNoji8MhFZkUB4DiWCCcz': -4863.519055
            },
            dict(map(
                lambda kv: (kv[0], kv[1].float),
                self._transaction_with_tokens.total_token_changes(BalanceChangeAgg.OUT).items()
            ))
        )

        self.assertEqual(
            {
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 12.884202,
                'EWS2ATMt5fQk89NWLJYNRmGaNoji8MhFZkUB4DiWCCcz': 4863.519055
            },
            dict(map(
                lambda kv: (kv[0], kv[1].float),
                self._transaction_with_tokens.total_token_changes(BalanceChangeAgg.IN).items()
            ))
        )

        self.assertEqual(
            {
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                'EWS2ATMt5fQk89NWLJYNRmGaNoji8MhFZkUB4DiWCCcz'
            },
            self._transaction_with_tokens.mints
        )
