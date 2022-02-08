from __future__ import annotations

import json
from enum import Enum
from typing import Iterable, Set, List, Tuple, Callable

from pandas import DataFrame

from src.transform.AccountType import AccountType
from src.transform.BalanceChange import BalanceChangeAgg
from src.transform.Block import Block
from src.transform.Interactions import Interactions
from src.transform.Transfer import Transfer

ResultsAndErrors = Tuple[List[List[any]], List[List[any]]]
Transform = Callable[[Block], ResultsAndErrors]


def block_to_transactions(block: Block) -> ResultsAndErrors:
    rows = []
    errors = []

    for transaction in block.transactions:
        try:
            rows.append([
                block.epoch,
                transaction.signature,
                transaction.fee,
                transaction.is_successful,
                len(transaction.instructions),
                json.dumps(list(map(lambda a: a.key, transaction.instructions.programs))),
                len(transaction.accounts),
                json.dumps({
                    account_type.name: [
                        a.key for a in accounts
                    ] for account_type, accounts in transaction.accounts_by_type().items()
                }),
                transaction.total_account_balance_change(BalanceChangeAgg.OUT).v,
                transaction.total_account_balance_change(BalanceChangeAgg.IN).v,
                json.dumps(len(transaction.mints)),
                json.dumps(list(transaction.mints)),
                json.dumps({mint: change.float for mint, change in
                            transaction.total_token_changes(BalanceChangeAgg.OUT).items()}),
                json.dumps({mint: change.float for mint, change in
                            transaction.total_token_changes(BalanceChangeAgg.IN).items()}),
                block.hash,
                str(block.source)
            ])
        except Exception as e:
            errors.append(['blocks_to_transactions', str(block.source), str(e)])

    return rows, errors


def block_to_transfers(block: Block) -> ResultsAndErrors:
    """
    For each block, return a tuple of rows of parsed transfers and rows of errors.
    """
    rows = []
    errors = []

    interactions = Interactions([block])
    for interaction in interactions:
        try:
            if isinstance(interaction, Transfer):
                rows.append([
                    block.epoch,
                    interaction.source,
                    interaction.destination,
                    interaction.mint,
                    interaction.value.v,
                    interaction.value.scale,
                    interaction.transaction_signature,
                    block.hash,
                    str(block.source)
                ])
        except Exception as e:
            errors.append(['blocks_to_transfers', str(block.source), str(e)])

    return rows, errors


def block_info(block: Block) -> ResultsAndErrors:
    row = [
        block.epoch,
        block.hash,
        str(block.source),
        len(block.transactions)
    ]

    for transactions in [block.transactions.successful, block.transactions.errors]:
        row.extend([
            len(transactions),
            len(transactions.votes),
            len(transactions.more_than_fee),
            len(transactions.only_fee),
            transactions.fees,
            transactions.balance_change(BalanceChangeAgg.OUT).v
        ])

        accounts_by_type = transactions.accounts_by_type
        for account_type in [AccountType.PROGRAM, AccountType.COIN, AccountType.TOKEN]:
            row.append(len(accounts_by_type.get(account_type, [])))

    return [row], []

class TransformTask(Enum):
    """
    Tasks that perform a set of transformations and returns a set of loadable results and metadata.

    @author zuyezheng
    """

    TRANSACTIONS = (
        block_to_transactions,
        [
            ('time', 'int64'),
            ('signature', 'string'),
            ('fee', 'int64'),
            ('isSuccessful', 'bool'),
            ('numInstructions', 'int8'),
            ('programs', 'str'),
            ('numAccounts', 'int8'),
            ('accountsByType', 'string'),
            ('lamportsOut', 'int64'),
            ('lamportsIn', 'int64'),
            ('numMints', 'int8'),
            ('mints', 'string'),
            ('tokensOut', 'string'),
            ('tokensIn', 'string'),
            ('blockhash', 'string'),
            ('path', 'string')
        ]
    )
    TRANSFERS = (
        block_to_transfers,
        [

            ('time', 'int64'),
            ('source', 'string'),
            ('destination', 'string'),
            ('mint', 'string'),
            ('value', 'int64'),
            ('scale', 'int8'),
            ('transaction', 'string'),
            ('blockhash', 'string'),
            ('path', 'string')
        ]
    )
    BLOCKS = (
        block_info,
        [
            ('time', 'int64'),
            ('hash', 'string'),
            ('path', 'string'),
            ('numTransactions', 'int64'),
            ('numSuccessful', 'int64'),
            ('successfulVotes', 'int64'),
            ('successfulTransactionsMoreThanFee', 'int64'),
            ('successfulTransactionsOnlyFee', 'int64'),
            ('successfulFees', 'int64'),
            ('successfulBalanceChange', 'int64'),
            ('successfulProgramAccounts', 'int64'),
            ('successfulCoinAccounts', 'int64'),
            ('successfulTokenAccounts', 'int64'),
            ('numErrors', 'int64'),
            ('errorVotes', 'int64'),
            ('errorTransactionsMoreThanFee', 'int64'),
            ('errorTransactionsOnlyFee', 'int64'),
            ('errorFees', 'int64'),
            ('errorBalanceChange', 'int64'),
            ('errorProgramAccounts', 'int64'),
            ('errorCoinAccounts', 'int64'),
            ('errorTokenAccounts', 'int64')
        ]
    )

    @staticmethod
    def all() -> Set[TransformTask]:
        return set([task for task in TransformTask])

    @staticmethod
    def from_names(names: Iterable[str]) -> Set[TransformTask]:
        tasks = set()
        for name in names:
            normalized_name = name.upper()
            if normalized_name == 'ALL':
                return TransformTask.all()
            else:
                tasks.add(TransformTask[normalized_name])

        return tasks

    @staticmethod
    def errors_to_df(errors: List[List[any]]) -> DataFrame:
        return DataFrame(errors, columns=['name', 'block', 'message'])

    transform: Transform
    meta: List[(str, str)]

    def __init__(self, transform: Transform, meta: List[(str, str)]):
        self.transform = transform
        self.meta = meta

    def to_df(self, rows: List[List[any]]) -> DataFrame:
        return DataFrame(rows, columns=list(map(lambda c: c[0], self.meta)))
