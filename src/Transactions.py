from typing import List

from src.Transaction import Transaction


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

    def more_than_fee(self) -> List[Transaction]:
        """ Transactions where absolute balance change was greater than the fee. """
        return list(filter(
            lambda t: t.total_account_balance_change() != t.fee(),
            self.transactions
        ))

    def only_fee(self) -> List[Transaction]:
        """ Transactions where only balance change was the fee. """
        return list(filter(
            lambda t: t.total_account_balance_change() == t.fee(),
            self.transactions
        ))

