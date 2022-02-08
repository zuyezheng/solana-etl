from __future__ import annotations

from functools import cached_property, reduce
from typing import List, Callable, Dict, Set

from src.transform.Account import Account
from src.transform.AccountType import AccountType
from src.transform.BalanceChange import BalanceChangeAgg
from src.transform.NumberWithScale import NumberWithScale
from src.transform.Transaction import Transaction


class Transactions:
    """
    Query utils for a collection of transactions.

    @author zuyezheng
    """

    transactions: List[Transaction]

    def __init__(self, transactions: List[Transaction]):
        self.transactions = transactions
        self.size = len(self.transactions)

    def __iter__(self):
        return self.transactions.__iter__()

    def __len__(self):
        return len(self.transactions)

    @property
    def fees(self) -> int:
        return reduce(lambda acc, t: acc + t.fee, self.transactions, 0)

    def balance_change(self, agg: BalanceChangeAgg) -> NumberWithScale:
        return reduce(
            lambda acc, t: acc + t.total_account_balance_change(agg),
            self.transactions,
            NumberWithScale.lamports(0)
        )

    def filter(self, f: Callable[[Transaction], bool]):
        return Transactions(list(filter(
            f, self.transactions
        )))

    @cached_property
    def successful(self) -> Transactions:
        """ Return successful transactions. """
        return self.filter(lambda t: t.is_successful)

    @property
    def errors(self) -> Transactions:
        """ Return errored transactions. """
        return self.filter(lambda t: not t.is_successful)

    @property
    def votes(self) -> Transactions:
        """ Transactions with vote instructions """
        return self.filter(lambda t: t.has_instruction_of('vote'))

    @property
    def more_than_fee(self) -> Transactions:
        """ Transactions where absolute balance change was greater than the fee. """
        return self.filter(lambda t: t.total_account_balance_change().v != -t.fee)

    @property
    def only_fee(self) -> Transactions:
        """ Transactions where only balance change was the fee. """
        return self.filter(lambda t: t.total_account_balance_change().v == -t.fee)

    @property
    def accounts_by_type(self) -> Dict[AccountType, Set[Account]]:
        aggregated_by_type = {}

        for transaction in self.transactions:
            transaction_accounts = transaction.accounts_by_type()
            for account_type, accounts in transaction_accounts.items():
                if account_type in aggregated_by_type:
                    aggregated_by_type[account_type].update(accounts)
                else:
                    aggregated_by_type[account_type] = accounts

        return aggregated_by_type

