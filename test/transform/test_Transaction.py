import unittest
from functools import reduce
from pathlib import Path
from typing import Dict, Set

from src.transform.AccountType import AccountType
from src.transform.BalanceChange import BalanceChangeAgg
from src.transform.Block import Block
from src.transform.Transaction import Transaction


class TestTransaction(unittest.TestCase):

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

    def test_account_types(self):
        def to_keys(accounts_by_type: Dict) -> Dict[AccountType, Set[str]]:
            transformed = dict()
            for account_type, accounts in accounts_by_type.items():
                transformed[account_type] = set(map(lambda a: a.key, accounts))

            return transformed

        def count(accounts_by_type: Dict) -> int:
            return reduce(
                lambda a, b: a + b,
                map(lambda accounts: len(accounts), accounts_by_type.values()),
            )

        self.assertEqual(
            {
                AccountType.SYSVAR: {
                    'SysvarC1ock11111111111111111111111111111111',
                    'SysvarRent111111111111111111111111111111111'
                },
                AccountType.PROGRAM: {
                    '11111111111111111111111111111111',
                    'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
                    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
                    'cndyAnrLdpjq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ',
                    'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
                },
                AccountType.TOKEN: {
                    '5KFsC5mLg6d2MbJgCs4k2qJVampgPoZFeuSwsUjSN2gJ'
                },
                AccountType.COIN: {
                    '4QuHa8NuHCFvx2XgYG5F5LyG8CpaHXXaDbn3ouv9khxh',
                    '6DLUecp4G13R4BCANcYZm3W3A55vm8ith7VscMAr8wV3',
                    '6vV7x9Gzrkd7HGds9JyQ3NuMNao3aVBJtyL1aJLM5CKY',
                    '7WK1nq1iCw6W2Da5PM5dihn8iCEvjBC3QAzEHKYFGfNY',
                    '7zAhomM86b2LCtRniiSUor1qEYSH2LWUBB4WJXCzeEts',
                    'E2HeNtruwL6bcd6XSqKGk5ucw43jrNsThFHoSmTNeSbi'
                }
            },
            to_keys(self._interesting_transaction.accounts_by_type)
        )

        # make sure all accounts are in by type
        self.assertEqual(
            len(self._interesting_transaction.accounts),
            count(self._interesting_transaction.accounts_by_type)
        )

        self.assertEqual(
            {
                AccountType.SYSVAR: set(),
                AccountType.PROGRAM: {
                    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
                    'CTMAxxk34HjKWxQ3QLZK1HpaLXmBveao3ESePXbiyfzh',
                    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                },
                AccountType.TOKEN: {
                    '5cR1yJcjMaHLAPMqXEZc6zaTazn1fiDctXG39crTUfq3',
                    '6H4TkDcHEWkyM2LVNkHdmBsZym4b7Hf5SYfq4HRMbtHR',
                    '6rQjE7ve9vmZw2L988mRCgWUJAeBGKLJHjq1oeFbf7Fb',
                    '7jMC3ZYQtRQycDwSTVKxyXhvpoU5C1T4ENJoYNjWLJ6T',
                    '8fEdArAuMR3b44WQK1UL1fDLB3kDK2N9whhDRP4sWP5v',
                    '9BnkuYqwYdrp7A8kV8V3uPRb21Kjihc7C8eg6jmMz1dm',
                    'AqWzsrvrTumzGNgLadaiu5Bz46xE4q3CZApQRUqsbbu9',
                    'DmmSN7NH3FpKSkfNuE2MbbWPoW1uVrFdJDVtrCeuo5Wi',
                    'GjKZHWYCikFFMLUozYLzCnR5kJp61LqQkLPEtv5aJB4k'
                },
                AccountType.COIN: {
                    '3PRUbriFa4UD6tdY5CjwYUQvhyvUEMTHev2UMaWh5MPR',
                    '3hsU1VgsBgBgz5jWiqdw9RfGU6TpWdCmdah1oi4kF3Tq',
                    '5CGnktbzR3xeyzmk29avbPnN23vCLSnjCXh9Wqvmo5r1',
                    '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1',
                    '68ebukwn1bqm8dTqduRyn4YipJWEFNTCkgMqCK2Mf4rK',
                    '7sPDo4E37dVwmFPU4Xba9YwzQHuEpYCsA9W4W9JkxBH8',
                    '8pYy9iGLajdiUw6WVaxZnhyrHoQV9nRRWkdbtr6FcYB4',
                    '9VwdsbBGkB7V2aBaGwcArKciuQUiM2ARtSZPSfTo3x2P',
                    '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin',
                    'Ai5ZrhuwvDiLubv7gNpVS1v9qrKo5kxfARZGtKagkvH9',
                    'DnG2KhjcNzFoxJVL3L4qRTxvgE9z6PNjeVaD6AkmFTfr',
                    'EGbaXYDVNghg6QHv9cpT7PBK1dQik9gg4vDvANB6Efi8',
                    'HchZzqewgC4pfsJU9uxFsrP4sLDqLsDntYRzE5RgrBak',
                    'JAPtCSJgwnuPCMtHMWWnsmuHLiqxGiHnmUe2UYw3edRY',
                    'ufLNV197ZaHDVX79ZTrMrEWy88AX4oC2msZkCfkRT2J'
                }
            },
            to_keys(self._transaction_with_tokens.accounts_by_type)
        )

        self.assertEqual(
            len(self._transaction_with_tokens.accounts),
            count(self._transaction_with_tokens.accounts_by_type)
        )
